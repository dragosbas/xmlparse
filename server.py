from flask import (Flask, request,jsonify,send_file)
import os,zipfile,time,xmltodict
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
        try:
            APPROVED_CUI=request.form.get('companyCui','None')
            PERIOADA=request.form.get('reportDate','2022-01-01')
            DISAPROVED_COR=request.form.get('corExclus','0001')
            FILE_REQUESTED=request.form.get('fileRequested','NO')
            lista_cnp_crypt=[cryptCNP(request.form.get('cnp1')),request.form.get('cnp2')]
            lista_cor_exclus=[DISAPROVED_COR]
            
            for filename in os.listdir(upload_folder):
                os.remove(os.path.join(upload_folder, filename))
            file=request.form.get('file',request.files['file'])
            #Unzip File 1
            with zipfile.ZipFile((file), mode='r') as my_zip:
                my_zip.extractall(upload_folder)
            my_zip.close()
        except:
            return(jsonify({"success":"none","error": "Initial container file is not a zip file"}))
        try:     
            #Unzip grouped xmls
            for filename in os.listdir(upload_folder):
                with zipfile.ZipFile(os.path.join(upload_folder, filename), mode='r') as my_zip:
                    my_zip.extractall(upload_folder)
                my_zip.close()
                os.remove(os.path.join(upload_folder, filename))
        except:
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
            return(jsonify({"error": "Problems parsing xml files"}))    
       
        try:
            processed_database=process(xmlData,lista_cnp_crypt=lista_cnp_crypt,lista_cor_exclus=lista_cor_exclus,perioada=PERIOADA,cui=CUI)
            raport+=f"--Time until xml processing ends : {time.time() - start_time}"
        except:
            return(jsonify({"error": "Problems processing xml files"}))
        
        querry_report=database_connection.insert(processed_database['tabele'])
        
        raport+=processed_database['raport']
        if FILE_REQUESTED=='YES': return send_file('revisalImportQuery.sql')
        return jsonify({"success": "File uploaded successfully", "time": time.time() - start_time,"error": "None",'raport':raport,'querry':raport})
    return '''
    <!doctype html>
    <title>Upload new File</title>
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
      <label for="minCor">Numar minim de CORuri pentru care se face uploadarea NOT YET IMPLEMENTED   </label><br>
      <input type=text name="minCor" value='1' ><br><br>
      <label for="fileRequested">Reply as file</label><br>
      <input type=text name="fileRequested" value='YES'><br><br>
      <input type=submit value=Upload>
    </form>
    <h2>Last Update : 9 Jun 2022 : 10:00</h2>
    '''
    
def cryptCNP(cnp):
    return cnp[::-1]

def add_id(list):
    for pos_in_list in range(0,len(list)):
        list[pos_in_list]['id']=pos_in_list
    return list

def filter_dict(dictionary):
    result={}
    for key in dictionary.keys():
        if dictionary[key]=={'@i:nil': 'true'}:result[key]='None'
        else:result[key]=dictionary[key]
    return result
            
def rename_dict_keys(source):
    # result = {}
    # for key in source.keys():
    #     result[key.lower()] = source[key]
    return source

def process(xmlData,lista_cnp_crypt=[],lista_cor_exclus=[],min_nr_cor=1,perioada='2022-01-01',cui=''):
    final_export_salariati=[]
    final_export_contracte=[]
    final_export_cor=[]
    final_export_stare_curenta=[]
    final_export_timp_munca=[]
    final_export_localitate=[]
    final_export_nationalitate=[]
    final_export_tara_domiciliu=[]
    final_export_detalii_salariati_straini=[]
    final_export_sporuri_salariu=[]
    for salariat in xmlData:
        salariat_export={'id':len(final_export_salariati),'perioada':perioada,'cui':cui}
        salariat_export['cnp']=cryptCNP(salariat.get('Cnp').split(',')[0])
        if salariat_export['cnp'] in lista_cnp_crypt:
            continue #sari peste cnp in ista lista
        
        salariat = filter_dict(salariat)
        
        # salariat_export['adresa']=salariat.get('Adresa')
        # salariat_export['nume']=salariat.get('Nume')
        salariat_export['apatrid']              =salariat.get('Apatrid')
        salariat_export['audit_entries']        =salariat.get('AuditEntries')
        salariat_export['luna_nastere']         =salariat.get('Cnp')[3:5]
        salariat_export['an_nastere']           =salariat.get('Cnp')[1:3]
        salariat_export['audit_entries']        =salariat.get('AuditEntries')
        salariat_export['tip_act_de_identitate']=salariat.get('TipActIdentitate')
        salariat_export['sex']="M" if salariat.get('Cnp') in ['1','5'] else 'F' 
        # salariat_export['cnp_vechi']=salariat.get('CnpVechi')

        detalii_salariat_strain=salariat.get('DetaliiSalariatStrain')
        if detalii_salariat_strain=="None":detalii_salariat_strain={}   
        detalii_salariat_export={'data_inceput_autorizatie':detalii_salariat_strain.get('DataInceputAutorizatie','None').split('T')[0]}
        detalii_salariat_export['data_sfarsit_autorizatie']=detalii_salariat_strain.get('DataSfarsitAutorizatie','None').split('T')[0]
        detalii_salariat_export['tip_autorizatie']=detalii_salariat_strain.get('TipAutorizatie','None')
        
        if detalii_salariat_export not in final_export_detalii_salariati_straini:
            final_export_detalii_salariati_straini.append(detalii_salariat_export)
        salariat_export['detalii_salariat_strain_id']=final_export_detalii_salariati_straini.index(detalii_salariat_export)
            
        localitate=salariat.get('Localitate')
        if localitate not in final_export_localitate:
            final_export_localitate.append(localitate)
        salariat_export['localitate_id']=final_export_localitate.index(localitate)
        
        nationalitate=salariat.get('Nationalitate')
        if nationalitate not in final_export_nationalitate:
            final_export_nationalitate.append(nationalitate)
        salariat_export['nationalitate_id']=final_export_nationalitate.index(nationalitate)
        
        tara_domiciliu=salariat.get('TaraDomiciliu')
        if tara_domiciliu not in final_export_tara_domiciliu:
            final_export_tara_domiciliu.append(tara_domiciliu)
        salariat_export['tara_domiciliu_id']=final_export_tara_domiciliu.index(tara_domiciliu)


        contracte_salariat=salariat.get('Contracte').get('Contract')
        if contracte_salariat.__class__.__name__ == 'list':
            contract_list=contracte_salariat
        else:
            contract_list=[contracte_salariat]
        
        for contract in contract_list:
            contract_export={'id':len(final_export_contracte),'salariat_id':salariat_export['id']}
            contract_export['radiat']=contract.get('Radiat')
            if contract_export['radiat']=='true': continue
            
            # filtru nepreluare contracte radiate

            contract=filter_dict(contract)
            contract_export['audit_entries']        =contract.get('AuditEntries')
            contract_export['data_consemnare']      =contract.get('DataConsemnare').split('T')[0]
            contract_export['data_inceput_contract']=contract.get('DataInceputContract').split('T')[0]
            contract_export['data_sfarsit_contract']=contract.get('DataSfarsitContract').split('T')[0]
            contract_export['date_contract_vechi']  =contract.get('DateContractVechi').split('T')[0]
            contract_export['detalii']              =contract.get('Detalii')
            contract_export['detalii_mutare']       =contract.get('DetaliiMutare')
            contract_export['exceptii_data_sfarsit']=contract.get('ExceptiiDataSfarsit')
            contract_export['numar_contract']       =contract.get('NumarContract')
            contract_export['numere_contract_vechi']=contract.get('NumereContractVechi')
            
            contract_export['tip_contract_munca']   =contract.get('TipContractMunca')
            contract_export['tip_durata']           =contract.get('TipDurata')       
            contract_export['tip_norma']            =contract.get('TipNorma')
            
            cor=contract.get('Cor')
            if cor.get('Cod') in lista_cor_exclus: continue # sari peste cnp din lista
            cor_export={'cod':cor.get('Cod'),'versiune':cor.get('Versiune')}
            if cor_export not in final_export_cor:
                final_export_cor.append(cor_export)
            contract_export['cor_id']=final_export_cor.index(cor_export)
            
            sporuri_salariu = contract.get('SporuriSalariu',[{"None":None}])
            if sporuri_salariu.__class__.__name__ == 'list':
                lista_sporuri=sporuri_salariu
            else:
                lista_sporuri=[sporuri_salariu]
            for spor in lista_sporuri:
                if spor=='None': continue
                spor_export={
                    'is_procent':spor.get('Spor',{}).get('IsProcent',"None"),
                    'valoare':spor.get('Spor',{}).get('Valoare',"None"),
                    'tip_spor':spor.get('Spor',{}).get('Tip',"None").get('@i:type',"None"),
                    'nume':spor.get('Spor',{}).get('Tip',"None").get('Nume',"None"),
                    'versiune':spor.get('Spor',{}).get('Tip',"None").get('Versiune',"None"),
                    'contract_id':contract_export['id']
                    }
                final_export_sporuri_salariu.append(spor_export)


            timp_munca=filter_dict(contract.get('TimpMunca'))
            timp_munca_export={'durata':timp_munca.get('Durata')}
            timp_munca_export['interval_de_timp']=timp_munca.get('IntervalDeTimp')
            timp_munca_export['norma']=timp_munca.get('Norma')
            timp_munca_export['repartizare']=timp_munca.get('Repartizare')

            if timp_munca_export not in final_export_timp_munca:
                final_export_timp_munca.append(timp_munca_export)
            contract_export['timp_munca_id']=final_export_timp_munca.index(timp_munca_export)
            
            stare_curenta=filter_dict(contract.get('StareCurenta'))
            stare_curenta_export={'type':stare_curenta.get('@i:type')}
            stare_curenta_export['data_incetare_detasare']=stare_curenta.get('DataIncetareDetasare','None').split('T')[0]
            stare_curenta_export['data_incetare_suspendare']=stare_curenta.get('DataIncetareSuspendare','None').split('T')[0]
            stare_curenta_export['stare_precedenta']=stare_curenta.get('StarePrecedenta','None')
            stare_curenta_export['data_incetare']=stare_curenta.get('DataIncetare','None').split('T')[0]
            stare_curenta_export['explicatie']=stare_curenta.get('Explicatie','None')
            stare_curenta_export['temei_legal']=stare_curenta.get('TemeiLegal','None')
            
            if stare_curenta_export not in final_export_stare_curenta:
                final_export_stare_curenta.append(stare_curenta_export)
            contract_export['stare_curenta_id']=final_export_stare_curenta.index(stare_curenta_export)

            final_export_contracte.append(contract_export)
        if len(contract_export.keys())==2:continue               #daca nu are contracte de munca, nu se adauga salariatul 
        salariat_export['upload_date']=time.strftime("%Y-%m-%d") #year - month - day_abbr
        final_export_salariati.append(rename_dict_keys(salariat_export))
    
    
    # removing less then cors
    

    #adding id's from position in list
    final_export_localitate=add_id(final_export_localitate)
    final_export_cor=add_id(final_export_cor)
    final_export_timp_munca=add_id(final_export_timp_munca)
    final_export_stare_curenta=add_id(final_export_stare_curenta)
    final_export_sporuri_salariu=add_id(final_export_sporuri_salariu)
    final_export_detalii_salariati_straini=add_id(final_export_detalii_salariati_straini)
    text_summary=''
    text_summary+=(f'\n----Numar de contracte munca------------- {len(final_export_contracte)}')
    text_summary+=(f'\n----Numar de sporuri--------------------- {len(final_export_sporuri_salariu)}')
    text_summary+=(f'\n----Numar de salariati------------------- {len(final_export_salariati)}')
    text_summary+=(f'\n----Numar de localitati------------------ {len(final_export_localitate)}')
    text_summary+=(f'\n----Numar de COR-uri distincte----------- {len(final_export_cor)}')
    text_summary+=(f'\n----Numar de Incadrari in TimpMunca------ {len(final_export_timp_munca)}')
    text_summary+=(f'\n----Numar de Stari curente distincte ---- {len(final_export_stare_curenta)}')
    text_summary+=(f'\n----Numar Detalii Salariati Straini ----- {len(final_export_detalii_salariati_straini)}')
    text_summary+=('\n------------------------------------------------')
    print(text_summary)
    # print(f'----Exemplu de contracte----- {final_export_contracte[0]}')
    # print(f'----Exemplu de salariati----- {final_export_salariati[0]}')
    # print(f'----Exemplu de localitati---- {final_export_localitate[0]}')
    # print(f'----Exemplu de COR----------- {final_export_cor[0]}')
    # print(f'----Exemplu de TimpMunca----- {final_export_timp_munca[0]}')
    # print(f'----Exemplu de Stare_curente- {final_export_stare_curenta[0]}')
    # print(f'----Exemplu DetaliiSalariati- {final_export_detalii_salariati_straini[0]}')
    
    return {'tabele':{
        'AAcontracte':final_export_contracte,
        'AAsalariati':final_export_salariati,
        'AAlocalitati':final_export_localitate,
        'AAcor':final_export_cor,
        'AAtimp_munca':final_export_timp_munca,
        'AAstare_curenta':final_export_stare_curenta,
        'AAdetalii_salariati_straini':final_export_detalii_salariati_straini,
        'AAsporuri_salariu':final_export_sporuri_salariu},
        'raport':text_summary}
 
if __name__ == "__main__":
    CORS(app.run(debug=True))
    print("Starting server...")


    
