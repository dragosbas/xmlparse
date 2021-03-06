from cmath import e
import re
from flask import (Flask, request,jsonify,send_file)
import os,time,xmltodict,hashlib,shutil
import database_connection
from flask_cors import CORS

#test Azure Database
# try:
#     if(database_connection.execute_query("select * from dbo.app_user")):
#         print("Azure Database is connected")
# except:
#     print("Azure Database is not connected")


app = Flask(__name__)
app.secret_key = 'sogard'
try:
    os.mkdir(os.path.join(os.path.dirname(__file__),"uploads"))
except:
    pass

app.config["UPLOAD_FOLDER"] = os.path.join(
    os.path.dirname(__file__), "uploads")

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        raport='--Raportul a fost generat in data de '+time.strftime("%d/%m/%Y")+' la ora '+time.strftime("%H:%M:%S")+'\n'
        start_time = time.time()
        upload_folder = app.config['UPLOAD_FOLDER']
        APPROVED_CUI=request.form.get('companyCui','')
        PERIOADA=request.form.get('reportDate','2022-01-01')
        DISAPROVED_COR=request.form.get('corExclus','0001')
        FILE_REQUESTED=request.form.get('fileRequested','NO')
        lista_cnp_crypt=[cryptCNP(request.form.get('cnp1')),request.form.get('cnp2')]
        lista_cor_exclus=[DISAPROVED_COR]
        try:
            MINCOR=int(request.form.get('minCor',"0"))
        except:
            MINCOR=1 
        
        for filename in os.listdir(upload_folder):
            os.remove(os.path.join(upload_folder, filename))
        
        try:
            uploaded_file=request.form.get('file',request.files['file'])
            uploaded_file.save(os.path.join(upload_folder, uploaded_file.name))
            shutil.unpack_archive(os.path.join(upload_folder, uploaded_file.name),os.path.join(upload_folder),"tar")
            os.remove(os.path.join(upload_folder, uploaded_file.name))
        except:
            return (jsonify({"success":"None","time":time.time() - start_time,'error':'Nu s-a putut procesa fisierul rvs'}))
        
        at_least_one_valid_file=False 
        for filename in os.listdir(upload_folder):
            try:
                shutil.unpack_archive(os.path.join(upload_folder, filename),os.path.join(upload_folder),"zip")
                at_least_one_valid_file=True
            except:
                pass
            os.remove(os.path.join(upload_folder, filename))
        if not at_least_one_valid_file:
            return(jsonify({"success":"None","time":time.time() - start_time,"error": "Container does not contain zip files"}))        
        
        xmlData=[]
        
        try:
            for filename in os.listdir(upload_folder):
                with open(os.path.join(upload_folder, filename), 'rt', encoding="utf8") as currentfile:
                    file_as_xml = xmltodict.parse(currentfile.read())
                    CUI=file_as_xml['XmlReport']['Header']['Angajator']['Detalii']['Cui']
                    if(CUI != APPROVED_CUI):
                        return(jsonify({"success":"None","time":time.time() - start_time,"error": "CUI does not match"}))
                    xmlData+=file_as_xml['XmlReport']['Salariati']['Salariat']
                    currentfile.close()
                os.remove(os.path.join(upload_folder, filename))
            raport+=f"--Time until xml merge end : {time.time() - start_time}"
        except:
            return(jsonify({"success":"None","time":time.time() - start_time,"error": "Problems merging xml files"}))    
       
        try:
            processed_database=process2(xmlData,lista_cnp_crypt=lista_cnp_crypt,lista_cor_exclus=lista_cor_exclus,perioada=PERIOADA,cui=CUI,minCor=MINCOR)
            raport+=f"--Time until xml processing ends : {time.time() - start_time}"
        except:
            return(jsonify({"error": "Problems processing xml files"}))
        
        querry_report=database_connection.insert(processed_database['tabele'])
        
        # raport+=processed_database['raport']
        if FILE_REQUESTED=='YES': return send_file('revisalImportQuery.sql')
        return jsonify({"success": "File uploaded successfully", "time": time.time() - start_time,"error": "None",'raport':raport,'querry':raport,'tables':processed_database.get('tabele',{})})
    return '''
    <!doctype html>
    <title>Upload RVS File</title>
    <h1>Upload new File </h1>
    <form method=post enctype=multipart/form-data><br>
      <input type=file name=file><br>
      <label for="corExclus">Numar COR de exclus:    </label><br>
      <input type=text name="corExclus" value=''><br>
      <label for="cnp1">CNP#1 de exclus (varianta necriptat):   </label><br>
      <input type=text name="cnp1" value=''><br>
      <label for="cnp2">CNP#2 de exclus (varianta criptat):   </label><br>
      <input type=text name="cnp2" value=''><br>
      <label for="companyCui">CUI companie la care utilizatorul are acces:   </label><br>
      <input type=text name="companyCui" value='27878713'><br>
      <label for="reportDate">Data pentru care se face raportul (se adauga la salariati pe coloana perioada):</label><br>
      <input type=text name="reportDate" value='2022-01-01'><br>
      <label for="minCor">Numar minim de CORuri pentru care se face uploadarea !   </label><br>
      <input type=text name="minCor" value='1' ><br><br>
      <label for="fileRequested">Reply as file</label><br>
      <input type=text name="fileRequested" value='YES'><br><br>
      <input type=submit value=Upload>
    </form>
    <h2>Last Update : 10 Jun 2022 : 21:00</h2>
    '''

def cryptCNP(cnp):
    return hashlib.sha256(cnp.encode()).hexdigest()
    
def add_id(list):
    for pos_in_list in range(0,len(list)):
        list[pos_in_list]['id']=pos_in_list
    return list

def filter_dict(dictionary):
    result={}
    for key in dictionary.keys():
        if dictionary[key]=={'@i:nil': 'true'}:result[key]=''
        else:result[key]=dictionary[key]
    return result
            
def rename_dict_keys(source):
    return source

def process2(xmlData,lista_cnp_crypt,lista_cor_exclus,perioada,cui,minCor=1):
    temp_export_salariati={} #de pastrat
    temp_export_contracte={} #de pastrat
    temp_export_sporuri_salariu={} #de pastrat
    try:
        IndexCol=f"{cui}_{perioada.split('-')[0]}_{perioada.split('-')[1]}"
    except:
        IndexCol=f"{cui}_1900_01"
    for salariat in xmlData:
        salariat_export={'Id':len(temp_export_salariati)+1,"IndexCol":f"{IndexCol}_{salariat['Cnp']}"}
        for restricted_field,item in filter_dict(salariat).items():
            if restricted_field!='Contracte':
                if item.__class__.__name__!='dict':
                    salariat_export[restricted_field]=item
                else:
                    for sub_item_key,sub_item_value in item.items():
                        salariat_export[f"{restricted_field}{sub_item_key}"]=sub_item_value
        temp_export_salariati[salariat_export.get('Id')]=salariat_export

        contracte_salariat=salariat.get('Contracte').get('Contract')
        if contracte_salariat.__class__.__name__ == 'list':
            contract_list=contracte_salariat
        else:
            contract_list=[contracte_salariat]
        
        for contract in contract_list:
            contract_export={'Id':len(temp_export_contracte)+1,'IdSalariat':salariat_export.get('Id'),"IndexCol":f"{IndexCol}_{contract.get('NumarContract','000')}"}
            contract=filter_dict(contract)
            for restricted_field,item in contract.items():
                if restricted_field!='SporuriSalariu':
                    if item.__class__.__name__!='dict':
                        contract_export[restricted_field]=item.split('T')[0]
                    else:
                        for sub_item_key,sub_item_value in filter_dict(item).items():
                            contract_export[f"{restricted_field}{sub_item_key.split('@')[0]}"]=sub_item_value
            temp_export_contracte[contract_export.get('Id',"")]=contract_export

            sporuri_salariu = contract.get('SporuriSalariu',"")
            if sporuri_salariu=='':
                continue
            
            if sporuri_salariu.__class__.__name__ == 'list':
                lista_sporuri=sporuri_salariu
            else:
                lista_sporuri=[sporuri_salariu]
            
            for spor in lista_sporuri:
                spor_export={
                    'Id':len(temp_export_sporuri_salariu)+1,
                    "IndexCol":IndexCol,
                    'IdContract':contract_export.get('Id',""),
                    'SporIsProcent':spor.get('Spor').get('IsProcent'),
                    'SporValoare':spor.get('Spor').get('Valoare'),
                    'SporTip':spor.get('Spor').get('Tip').get('@i:type'),
                    'SporNume':spor.get('Spor').get('Tip').get('Nume'),
                    'SporVersiune':spor.get('Spor').get('Tip').get('Versiune'),
                }
                temp_export_sporuri_salariu[spor_export.get('Id')]=spor_export
    id_salariati_export=set()
    id_contracte_export=set()
    id_sporuri_export=set()
    restricted_fields=['Nume',"Prenume","Adresa"]
    for id_salariat,salariat in temp_export_salariati.items():
        salariat['Cui']=cui
        salariat['Perioada']=perioada
        salariat['LunaNastere']=salariat.get('Cnp')[3:5]
        salariat['AnNastere']=salariat.get('Cnp')
        salariat['Sex']="M" if salariat.get('Cnp')[0] in ['1','5'] else 'F'
        salariat['Cnp']=cryptCNP(salariat.get('Cnp'))
        for restricted_field in restricted_fields:
            salariat.pop(restricted_field,"")
        if salariat['Cnp'] not in lista_cnp_crypt: 
            id_salariati_export.add(id_salariat) #nu preiau angajatii care au cnp invalid

    contract_cor_counts={}
    for id_contract,contract in temp_export_contracte.items():
        contract['Cui']=cui
        contract['Perioada']=perioada
        for restricted_field in restricted_fields:
            contract.pop(restricted_field,"")
        if contract.get('IdSalariat') not in id_salariati_export or contract.get("Radiat")=='true':
            continue
        id_contracte_export.add(id_contract)
        if contract_cor_counts.get(contract.get('CorCod'))==None:
            contract_cor_counts[contract.get('CorCod')]=1
        else:
            contract_cor_counts[contract.get('CorCod')]+=1

    for id_contract,contract in temp_export_contracte.items():
        if contract_cor_counts.get(contract.get('CorCod'))<minCor:
            id_salariati_export.discard(contract.get('IdSalariat')) # scot angajatii care au contracte sub minimc cor
            id_contracte_export.discard(id_contract) # scot contracte sub minim cor

    for id_spor,spor in temp_export_sporuri_salariu.items():
        spor['Cui']=cui
        spor['Perioada']=perioada
        for restricted_field in restricted_fields:
            spor.pop(restricted_field,"")
        if spor.get('IdContract') in id_contracte_export: id_sporuri_export.add(id_spor)
    export_salariati=[]
    export_contracte=[]
    export_sporuri_salariu=[]
    for id_salariat in id_salariati_export:
        export_salariati.append(temp_export_salariati.get(id_salariat))
    for id_contract in id_contracte_export:
        export_contracte.append(temp_export_contracte.get(id_contract))
    for id_spor in id_sporuri_export:
        export_sporuri_salariu.append(temp_export_sporuri_salariu.get(id_spor))
    
    return {'tabele':{
        "AAsalariati":export_salariati,
        "AAcontracte":export_contracte,
        "AAsporuri":export_sporuri_salariu}}

if __name__ == "__main__":
    CORS(app.run(debug=True))
    print("Starting server...")


    
