import os,time,xmltodict,hashlib,shutil,json,io
from flask import (Flask, request,jsonify,send_file)
from flask_cors import CORS
import pandas as pd

app = Flask(__name__)
app.secret_key = 'sogard'
app.config["UPLOAD_FOLDER"] = os.path.join(
    os.path.dirname(__file__), "uploads")

try:
    os.mkdir(os.path.join(os.path.dirname(__file__),"uploads"))
except:
    pass

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        raport='--Raportul a fost generat in data de '+time.strftime("%d/%m/%Y")+' la ora '+time.strftime("%H:%M:%S")+'\n'
        result={'filename':'','error':True,'success':False,'is_d112':False,'is_rvs':False}
        start_time = time.time()
        upload_folder = app.config['UPLOAD_FOLDER']
        APPROVED_CUI=request.form.get('companyCui','')
        PERIOADA=request.form.get('reportDate','2022-01-01')
        DISAPROVED_COR=request.form.get('corExclus','0001')
        FILE_REQUESTED=request.form.get('fileRequested','JSON')
        lista_cnp_crypt=[cryptCNP(request.form.get('cnp1', "")),request.form.get('cnp2', "")]
        BANNEDCNP = json.loads(request.form.get('bannedCnp',"[]"))
        lista_cnp_crypt+=BANNEDCNP
        lista_cor_exclus=[DISAPROVED_COR]
        try:
            MINCOR=int(request.form.get('minCor',"0"))
        except:
            MINCOR=1 
    
        for filename in os.listdir(upload_folder):
            os.remove(os.path.join(upload_folder, filename))
        
        try:
            uploaded_file=request.form.get('file',request.files['file'])
            uploaded_file.save(os.path.join(upload_folder, uploaded_file.filename))
        except:
            return Flask.response_class("No file attached !", status=401, mimetype='application/json')
        
        try:
            shutil.unpack_archive(os.path.join(upload_folder, uploaded_file.filename),os.path.join(upload_folder),"tar")
            os.remove(os.path.join(upload_folder, uploaded_file.filename))
            result['is_rvs']=True
        except:
            result['is_rvs']=False

        at_least_one_valid_file=False 
        if result['is_rvs']:
            for filename in os.listdir(upload_folder):
                try:
                    shutil.unpack_archive(os.path.join(upload_folder, filename),os.path.join(upload_folder),"zip")
                    at_least_one_valid_file=True
                except:
                    pass
                os.remove(os.path.join(upload_folder, filename))
            if not at_least_one_valid_file:
                return Flask.response_class("RVS FILE INCOMPLETE !", status=401, mimetype='application/json')
        
        xmlData=[]
        if result['is_rvs']:
            try:
                for filename in os.listdir(upload_folder):
                    with open(os.path.join(upload_folder, filename), 'rt', encoding="utf8") as currentfile:
                        file_as_xml = xmltodict.parse(currentfile.read())
                        CUI=file_as_xml.get('XmlReport',{}).get('Header',{}).get('Angajator',{}).get('Detalii',{}).get('Cui','0001')
                        if CUI!=APPROVED_CUI:
                            return Flask.response_class("CUI does not match", status=401, mimetype='application/json')
                        xmlData+=file_as_xml.get('XmlReport',{}).get('Salariati',{}).get('Salariat',[])
                        currentfile.close()
                    os.remove(os.path.join(upload_folder, filename))
                raport+=f"--Time until xml merge end : {time.time() - start_time}"
            except:
                return Flask.response_class("XML merge failuare", status=401, mimetype='application/json')
        else:
            try:
                with open(os.path.join(upload_folder, uploaded_file.filename), 'rt', encoding="utf8") as currentfile:
                    file_as_xml = xmltodict.parse(currentfile.read(),xml_attribs=True)
                    CUI=file_as_xml.get('declaratieUnica',{}).get('angajator',{}).get('@cif','0001')
                    if CUI!=APPROVED_CUI:
                        return Flask.response_class("CUI does not match", status=401, mimetype='application/json')
                    xmlData=file_as_xml.get('declaratieUnica',{})
                    currentfile.close()
                os.remove(os.path.join(upload_folder, uploaded_file.filename))
                result['is_d112']=True
            except:
                return Flask.response_class("XML parse failuare", status=401, mimetype='application/json')       

        if result['is_rvs']:
            # try:
                processed_database=process2(xmlData,lista_cnp_crypt=lista_cnp_crypt,lista_cor_exclus=lista_cor_exclus,perioada=PERIOADA,cui=CUI,minCor=MINCOR)
            # except:
            #     return Flask.response_class("Problems processing xml files from rvs", status=401, mimetype='application/json')        
        if result['is_d112']:
            # try:
                processed_database=process1(xmlData,lista_cnp_crypt=lista_cnp_crypt,lista_cor_exclus=lista_cor_exclus,perioada=PERIOADA,cui=CUI,minCor=MINCOR)
            # except:
            #     return Flask.response_class("Problems processing xml files from xmls", status=401, mimetype='application/json')        
        
        if FILE_REQUESTED not in ['SQL','XLSX']: return jsonify(processed_database.get('tabele',{}))
        
        generate_export_files(processed_database['tabele'])
        return_data = io.BytesIO()
        
        if FILE_REQUESTED=='SQL':
            # return send_file('revisalImportQuery.sql')
            file_path = 'output.sql'
        elif FILE_REQUESTED=='XLSX':
            file_path = 'output.xlsx'
            # return send_file('revisalImport.xlsx')

        with open(file_path, 'rb') as fo:
            return_data.write(fo.read())
        return_data.seek(0) # (after writing, cursor will be at last byte, so move it to start)

        os.remove('output.xlsx')
        os.remove('output.sql')

        return send_file(return_data,
                        mimetype=f'application/{file_path.split(".")[1]}',
                        as_attachment=True,
                        download_name=f'output.{file_path.split(".")[1]}')
        

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
            <input type="radio" name="fileRequested" id="option1" value="SQL" checked>Generate Report as SQL</input><br>
            <input type="radio" name="fileRequested" id="option2" value="XLSX">Generate Report as XLSX</input><br>
            <input type="radio" name="fileRequested" id="option3" value="JSON">Generate Report as JSON</input><br>
            <input type=submit value=Upload>
        </form>
        <h2>Last Update : 25 Jun 2022 : 13:00</h2>
    '''

def generate_export_files(import_data):
    querry = ""
    for table_name, table_data in import_data.items():
        querry += f"\nDROP TABLE IF EXISTS {table_name} ;\nCREATE TABLE {table_name} ("
        for column_name, column_type in table_data[0].items():
            querry += f"{column_name} VARCHAR(250),"
        querry = querry[:-1]+");"
        for row in table_data:
            row_columns=""
            row_values=""
            for column_name, column_value in row.items():
                row_columns += f"{column_name},"
                row_values += f"'{column_value}',"  
            querry+= f"\nINSERT INTO {table_name} ({row_columns[:-1]}) VALUES({row_values[:-1]});"

    with open("output.sql", "w",encoding="utf-8") as text_file:
        print(f"{querry}", file=text_file)
    
    with pd.ExcelWriter(f'output.xlsx') as writer:
        for name,data in import_data.items():
            pd.DataFrame(data).to_excel(writer,sheet_name=name)        

    # try:
    #     server = 'aibest.database.windows.net'
    #     database = 'aibest'
    #     username = 'robert'
    #     password = '{Dragos123}'
    #     driver = '{ODBC Driver 17 for SQL Server}'
    #     connection_string = 'DRIVER='+driver+';SERVER=tcp:'+server + \
    #         ';PORT=1433;DATABASE='+database+';UID='+username+';PWD=' + password

    #     with pymssql.connect(server=server, user=username, password='Dragos123', database=database) as conn:
    #         with conn.cursor() as cursor:
    #             print(f'Sql query execution starting in : {time.time()-start_time}')
    #             cursor.execute(querry)
    #             print(f'Sql query execution finished in : {time.time()-start_time}')
    # except:
    #     querry+="-- insertul nu a mers ! "
    return querry

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

def process1(xmlData={},lista_cnp_crypt=[],lista_cor_exclus=[],perioada='2000-01',cui='0001',minCor=1):
    campuri_retrictionate=['@numeAsig','@prenAsig']
    export_angajator=[]
    angajator_simple_keys={}
    
    for detaliu_angajator,valoare in xmlData.get('angajator',{}).items():
        if valoare=='': continue
        current_anganjator={'Id':len(export_angajator)+1,'CUI':cui,'Perioada':perioada}|angajator_simple_keys
        if "@" in detaliu_angajator:
            angajator_simple_keys[detaliu_angajator.replace('@','')]=valoare
            continue
        else: 
            valoare_as_list=[valoare] if type(valoare)==dict else valoare
            for row in valoare_as_list:
                for key,value in row.items():
                    current_anganjator[f"{detaliu_angajator.replace('@','')}_{key.replace('@','')}"]=value          
        export_angajator.append(current_anganjator)
    export_asigurat_keys=set()
    export_asigurat=[]
    asigurati=xmlData.get('asigurat',{})
    for asigurat in asigurati:
        current_asigurat={'Id':len(export_asigurat)+1,'CUI':cui,'Perioada':perioada}
        for detaliu_asigurat,valoare in asigurat.items():
            if detaliu_asigurat in campuri_retrictionate: continue
            if "@" in detaliu_asigurat:
                if detaliu_asigurat.find('@cnp')!=-1:
                    valoare = cryptCNP(valoare)
                    if valoare in lista_cnp_crypt: valoare=''
                    current_asigurat[detaliu_asigurat.replace('@','')]=valoare
                else: current_asigurat[detaliu_asigurat.replace('@','')]=valoare
                export_asigurat_keys.add(detaliu_asigurat.replace('@',''))
            else:
                if type(valoare)==dict: valoare=[valoare]
                for sublist in valoare:
                    for new_key,new_value in sublist.items():
                        if new_key.find('@')!=-1:new_key=new_key.replace('@','')
                        if new_key not in current_asigurat.keys():
                            current_asigurat[new_key]=[new_value]
                        else:
                            current_asigurat[new_key]+=[new_value]
                        export_asigurat_keys.add(new_key)
        if current_asigurat.get('cnpAsig')!='': export_asigurat.append(current_asigurat)
    
    SINGLEKEYS=['idAsig','CUI','Perioada','cnpAsig','dataAng','dataSf','A_1','A_2','A_3','A_4','A_5','A_6','A_7','A_8','C_1']
    MINKEYS=['B1_1','B1_3','E3_1','E3_2','E3_4']
    SUMKEYS=['B1_2','B1_4','B1_5','B1_6','B1_7','B1_8','B2_2','B2_3','B2_4','B2_5','B2_6','B2_7','B2_6i','B2_6f','B2_7i','B2_7f','B3_1','B3_2','B3_3','B3_4','B3_5','B3_6','B3_7','B3_8','B3_9','B3_10','B3_11','B3_12','B3_13','D_14','D_15','D_20','D_21','E3_8','E3_10','E3_16']
    final_export_asigurat=[]

    for asigurat in export_asigurat:
        final_asigurat={}
        for key in SINGLEKEYS:
            temp_value=asigurat.get(key,"")
            final_asigurat[key]=temp_value[0] if isinstance(temp_value,list) else temp_value
        for key in SUMKEYS:
            lista=list(map(int,asigurat.get(key,[])))
            total = '' if len(lista)==0 else f"{sum(lista)}"
            final_asigurat[key]=total
        for key in MINKEYS:
            minimum = "" if asigurat.get(key,-1)==-1 else min(asigurat.get(key,["0"]))
            final_asigurat[key]=f'{minimum}'
        final_export_asigurat.append(final_asigurat)
                
    return {'tabele':{
    "angajator":export_angajator,
    "asigurat":final_export_asigurat,
    }}
    
def process2(xmlData,lista_cnp_crypt,lista_cor_exclus,perioada,cui,minCor=1):
    temp_export_salariati={} #de pastrat
    temp_export_contracte={} #de pastrat
    temp_export_sporuri_salariu={} #de pastrat
    
    for salariat in xmlData:
        salariat_export={'Id':len(temp_export_salariati)+1}
        # salariat_export={'Id':uuid.uuid1()}
        salariat_export['Apatrid']=salariat.get('Apatrid',"")
        salariat_export['AuditEntries']=salariat.get('AuditEntries',"")
        salariat_export['LocalitateCodSiruta']=salariat.get('Localitate',{}).get('CodSiruta',"")
        salariat_export['NationalitateNume']=salariat.get('Nationalitate',{}).get('Nume',"")
        salariat_export['LunaNastere']=salariat.get('Cnp')[3:5]
        salariat_export['AnNastere']=salariat.get('Cnp')[1:3]
        salariat_export['Sex']="M" if salariat.get('Cnp')[0] in ['1','5'] else 'F'
        salariat_export['Cnp']=cryptCNP(salariat.get('Cnp'))
        salariat_export['Cui']=cui
        salariat_export['Perioada']=perioada
        for key in salariat_export.keys():
            if salariat_export[key]=={'@i:nil': 'true'}:salariat_export[key]=''
            if 'Data' in key: salariat_export[key]=salariat_export[key].split('T')[0]

        temp_export_salariati[salariat_export.get('Id')]=salariat_export

        contracte_salariat=salariat.get('Contracte').get('Contract')
        if contracte_salariat.__class__.__name__ == 'list':
            contract_list=contracte_salariat
        else:
            contract_list=[contracte_salariat]
        
        for contract in contract_list:
            contract_export={'Id':len(temp_export_contracte)+1,'IdSalariat':salariat_export.get('Id'),'Perioada':perioada,'Cui':cui}
            # contract_export={'Id':uuid.uuid1(),'IdSalariat':salariat_export.get('Id'),'Perioada':perioada,'Cui':cui}
            
            contract_export['AuditEntries ']=contract.get('ContractNume',"")
            contract_export['CorCod']=contract.get('Cor',{}).get('Cod',"")
            contract_export['CorVersiune']=contract.get('Cor',{}).get('Versiune',"")
            contract_export['DataConsemnare']=contract.get('DataConsemnare',"")
            contract_export['DataContract']=contract.get('DataContract',"")
            contract_export['DataInceputContract']=contract.get('DataInceputContract',"")
            contract_export['DataSfarsitContract']=contract.get('DataSfarsitContract',"")
            contract_export['ExceptieDataSfarsit']=contract.get('ExceptieDataSfarsit',"")
            contract_export['NumarContract']=contract.get('NumarContract',"")
            contract_export['Radiat']=contract.get('Radiat',"")
            contract_export['Salariu']=contract.get('ContractNume',"")
            contract_export['StareCurenta']=contract.get('StareCurenta',"").get('@i:type',"")
            contract_export['StareCurentaDataIncetareDetasare']=contract.get('StareCurenta',{}).get('DataIncetareDetasare',"")
            contract_export['StareCurentaDataIncetareSuspendare']=contract.get('StareCurenta',{}).get('DataIncetareSuspendare',"")
            contract_export['StareCurentaStarePrecedenta']=contract.get('StareCurenta',{}).get('StarePrecedenta',"")
            contract_export['StareCurentaDataIncetare']=contract.get('StareCurenta',{}).get('DataIncetare',"")
            contract_export['StareCurentaTemeiLegal']=contract.get('StareCurenta',{}).get('TemeiLegal',"")
            contract_export['StareCurentaDataInceput']=contract.get('StareCurenta',{}).get('DataInceput',"")
            contract_export['StareCurentaDataSfarsit']=contract.get('StareCurenta',{}).get('DataSfarsit',"")
            contract_export['TimpMuncaNorma']=contract.get('TimpMunca',{}).get('Norma',"")
            contract_export['TimpMuncaIntervalTimp']=contract.get('TimpMunca',{}).get('IntervalTimp',"")
            contract_export['TimpMuncaRepartizare']=contract.get('TimpMunca',{}).get('Repartizare',"")
            contract_export['TimpMuncaDurata']=contract.get('TimpMunca',{}).get('Durata',"")
            contract_export['TipContract']=contract.get('TipContract',"")
            contract_export['TipDurata']=contract.get('TipDurata',"")
            contract_export['TipNorma']=contract.get('TipNorma',"")

            for key in contract_export.keys():
                if contract_export[key]=={'@i:nil': 'true'}:contract_export[key]=''
                if 'Data' in key: contract_export[key]=contract_export[key].split('T')[0]
        
            temp_export_contracte[contract_export.get('Id',"")]=contract_export

            sporuri_salariu = contract.get('SporuriSalariu',"")
            if sporuri_salariu=={'@i:nil': 'true'}:
                continue
            
            if sporuri_salariu.__class__.__name__ == 'list':
                lista_sporuri=sporuri_salariu
            else:
                lista_sporuri=[sporuri_salariu]
            
            for spor in lista_sporuri:
                spor_export={
                    'Id':len(temp_export_sporuri_salariu)+1,
                    # 'Id':uuid.uuid1(),
                    'IdContract':contract_export.get('Id',""),
                    'SporIsProcent':spor.get('Spor',{}).get('IsProcent'),
                    'SporValoare':spor.get('Spor',{}).get('Valoare'),
                    'SporTip':spor.get('Spor',{}).get('Tip').get('@i:type'),
                    'SporNume':spor.get('Spor',{}).get('Tip').get('Nume'),
                    'SporVersiune':spor.get('Spor',{}).get('Tip').get('Versiune'),
                    'Cui':cui,
                    'Perioada':perioada
                }
                temp_export_sporuri_salariu[spor_export.get('Id')]=spor_export
    
    id_salariati_export=set()
    id_contracte_export=set()
    id_sporuri_export=set()
    
    for id_salariat,salariat in temp_export_salariati.items():
        if salariat['Cnp'] not in lista_cnp_crypt: 
            id_salariati_export.add(id_salariat) #nu preiau angajatii care au cnp invalid
        
    contract_cor_counts={}
    for id_contract,contract in temp_export_contracte.items():
        if contract.get('IdSalariat') not in id_salariati_export or contract.get("Radiat")=='true' or contract.get('CorCod',"") in lista_cor_exclus:
            continue
        id_contracte_export.add(id_contract)
        if contract_cor_counts.get(contract.get('CorCod'))==None:
            contract_cor_counts[contract.get('CorCod')]=1
        else:
            contract_cor_counts[contract.get('CorCod')]+=1
    
    for id_contract,contract in temp_export_contracte.items():
        if contract_cor_counts.get(contract.get('CorCod'),0)<minCor:
            id_salariati_export.discard(contract.get('IdSalariat')) # scot angajatii care au contracte sub minimc cor
            id_contracte_export.discard(id_contract) # scot contracte sub minim cor
    
    id_salariati_cu_contract=set()
    for id_contract in id_contracte_export:
        id_salariati_cu_contract.add(temp_export_contracte[id_contract].get('IdSalariat'))

    id_salariati_export= set(x for x in id_salariati_export if x in id_salariati_cu_contract)
    
    for id_spor,spor in temp_export_sporuri_salariu.items():
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
        "AAsporuri":export_sporuri_salariu,
        }}


if __name__ == "__main__":
    CORS(app.run(debug=True))
    print("Starting server...")


    
