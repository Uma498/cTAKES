import os
import json
import datetime
import shutil
import subprocess, random, string
from distutils.dir_util import copy_tree
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient

#Initialize folder names
dirName = 'tmp'
randomFolder = 'random'
inputFolder = 'cTakesExample'
configFolder = 'ctakes-config'
configFolder2 = 'ctakes-config2'
cData = 'cData'

#Source Input Data path
srcInputFolder = 'K:/tmp/ctakesExample/cData'  
#destination input data path
#destInputFolder = '/usr/app/tmp/ctakesExample/cData'

#source resources data path
srcResourcesFolder = '/usr/app/cTAKES/resources'
#destination resources data path
destResourcesFolder = '/usr/app/tmp/ctakes-config'

overviewdirectory = 'overview'
granulardirectory = 'granular'

CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=mystoragesamples;AccountKey=RdV/8wVWZa6Gu4czXf5Uy+ojefEhGCkZluISJxRWZeDhqAPv2ks3PMJhmhD+D5dtr76Nn1HoEXEdhs93pnRa9A==;EndpointSuffix=core.windows.net"
 

#uploading files to datalake details
storage_account_name = "datalakeaccount181255"
storage_account_key = "ae6WrFDeLTLficMNdbpK5pVVJAmlFbvJObwo9Uuicr0ikR5glf9IEjuKGTu9XgI1gUwnnoG62gnEBZoBMKt/NQ=="
container_name = "datalakecontainer"
directory_name = "datalakedirectory"

        
def totalCtakesProcess():
    os.chdir('/usr/app/')
    # checking of dirName exists or not
    if os.path.isdir(dirName):
        # remove the dirName
        shutil.rmtree(dirName)
        
    
    #create dirName(tmp folder)
    os.mkdir(dirName)
    #change dirName(/tmp) folder
    os.chdir(dirName)
    
    #create random folder in tmp
    os.mkdir(randomFolder)
    #create cTakesExample folder in tmp
    os.mkdir(inputFolder)
    #create ctakes-config folder in tmp
    os.mkdir(configFolder)
    #create ctakes-config2 folder in tmp
    os.mkdir(configFolder2)
    
    os.chdir(inputFolder)
    os.mkdir(cData)
    
    #environment variable 
    #message = str(os.environ['blobDownload']) 
    #message = str('blobinputcontainer/11111.txt/False') 
    #print(message) 

    #get tha container name, blob name and jobpipline value
    #container_name = message.split('/')[0]
    #print(container_name)
    #blob_name = message.split('/')[1]
    #print(blob_name)
    #jobType = message.split('/')[2]
    #print(jobType)
    
    #copy the files from source to destination input folders
    #copy_tree(srcInputFolder,destInputFolder)
    #copy the files from source to destination resources folders
    copy_tree(srcResourcesFolder,destResourcesFolder)
    
    
    #initialize
    jobType = 'True'
    
    #assumptions
    if jobType == 'True' :
        jobType == 'daily'
    else:
        jobType == 'research'
        
    #download blob from container
    #source_blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    #source_container_client = source_blob_service_client.get_container_client(container_name)
    #source_blob_client = source_container_client.get_blob_client(blob_name)
    
    #after working of environment variable  we are the follow from this
    #for downloading the files(blob)
    #blob_data = source_blob_client.download_blob()
    #print(blob_data)
    
    #read all the content from file
    #data = json.loads(blob_data.readall())
    #print(data)

    #for i in data :
        #filename = i['pat_enc_csn_id'] +"_" + i['note_id'] + ".json"
        #content = i['rawText']
        #print(filename)
        #print(content)
    
        #os.chdir("/usr/app/tmp/cTakesExample/cData")
        #with open(filename,"w") as file:
            #file.write(content)
    
    os.chdir(srcInputFolder)
    files = os.listdir()
    #print(files)
    for file in files:
        #change srcInput directory
        os.chdir(srcInputFolder)
        f = open(file,"r")
        content = f.read()
    
        datafile_name=''.join(random.choices(string.ascii_lowercase, k = 5))
        #print(datafile_name)
    
        datafile_number = int(random.random() * 10000)
        #print(datafile_number)

        filename = datafile_name + "_" + str(datafile_number) + '.json'
        
        os.chdir('/usr/app/tmp/ctakesExample/cData')
        
        with open(filename, 'w') as filenames :
            filenames.write(content)

        
    #move to /usr/app
    os.chdir('../')
    os.chdir("/usr/app/cTAKES")
    
    if jobType == 'True': 
        subprocess.run('mvn exec:java -Dexec.mainClass="org.apache.ctakes.pipelines.RushNiFiPipeline" -Dexec.args="--input-dir /usr/app/tmp/cTakesExample/cData --masterFolder /usr/app/tmp/ctakes-config/ --output-dir /usr/app/tmp/cTakesExample/ --tempMasterFolder /usr/app/tmp/ctakes-config2/ --jobPipline daily"', shell=True, check=True)
    else:
        subprocess.run('mvn exec:java -Dexec.mainClass="org.apache.ctakes.pipelines.RushNiFiPipeline" -Dexec.args="--input-dir /usr/app/tmp/cTakesExample/cData --masterFolder /usr/app/tmp/ctakes-config/ --output-dir /usr/app/tmp/cTakesExample/ --tempMasterFolder /usr/app/tmp/ctakes-config2/ --jobPipline research"', shell=True, check=True)
    
    os.chdir('/usr/app/')
    os.chdir(dirName)
    #changing to cTakesExample path
    os.chdir(inputFolder)
    
    #checking of output directories existed or not
    if os.path.isdir(overviewdirectory) or os.path.isdir(granulardirectory):
        print('cTakes process has been completed successfully')
    else :
        return 'overview and granular folders not created'
    
    #upload files to datalake
    service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)

    
    if os.path.isdir(overviewdirectory):
        #change overview path
        os.chdir(overviewdirectory)
        listoffiles = os.listdir()
        
        for file in listoffiles:
            f = open(file,"r")
            data = json.load(f)
            #print(data)
            data['fname'] = file 
            date = datetime.date.today()
            #print(date)
            data['loadTimestamp'] = date.strftime('%Y-%m-%d')
            #print(data)
            filename = open(file,"w")
            json.dump(data,filename)
            filename.close()
                
            #file_system_client = service_client.get_file_system_client(file_system=container_name)
            #directory_client = file_system_client.get_directory_client(directory_name)
            #file_client = directory_client.create_file(file)
            
            #open the file and read the content
            #local_file = open(file,'r')
            #file_contents = local_file.read()
            #push the data to datalake
            #file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
            #file_client.flush_data(len(file_contents))


    #print(os.getcwd())
    os.chdir('/usr/app/')
    os.chdir(dirName)
    #changing to cTakesExample path
    os.chdir(inputFolder)
    
    if os.path.isdir(granulardirectory):
        #change overview path
        os.chdir(granulardirectory)
        listoffiles = os.listdir()

        for file in listoffiles:
            f = open(file,"r")
            outputdata = json.load(f)
            #print(data)
            outputfile = []
            for data in outputdata:
                #print(data)
                data['fname'] = file 
                date = datetime.date.today()
                #print(date)
                data['loadTimestamp'] = date.strftime('%Y-%m-%d')
                #print(data)
                outputfile.append(data)
                
                filename = open(file,"w")
                json.dump(data,filename)
                filename.close()
                

                
            #file_system_client = service_client.get_file_system_client(file_system=container_name)
            #directory_client = file_system_client.get_directory_client(directory_name)
            #file_client = directory_client.create_file(file)
            
            #open the file and read the content
            #local_file = open(file,'r')
            #file_contents = local_file.read()
            #push the data to datalake
            #file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
            #file_client.flush_data(len(file_contents))
    
totalCtakesProcess()