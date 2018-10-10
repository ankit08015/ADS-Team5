import urllib.request
from bs4 import BeautifulSoup
import csv 
import logging 
import os
import zipfile
import boto.s3
import sys
from boto.s3.key import Key
import time
import datetime
from configparser import ConfigParser



log_file = logging.getLogger()
log_file.setLevel(logging.DEBUG)

log_copy = logging.FileHandler('log_file_1.log')
log_copy.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_copy.setFormatter(formatter)
log_file.addHandler(log_copy)

log_console = logging.StreamHandler(sys.stdout ) 
log_console.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s - %(message)s')
log_console.setFormatter(formatter)
log_file.addHandler(log_console)


argLen=len(sys.argv)

#print ("Please input the S3 Access Key")
#accessKey = input()
#logging.info("Access Key = %s" % accessKey)
#
#print ("Please input the S3 Secret Access Key")
#secretAccessKey = input()
#logging.info("Secret Access Key = %s" % secretAccessKey)
#
#
#print ("Please input your location")
#inputLocation = input()
#if inputLocation not in ['APNortheast', 'APSoutheast', 'APSoutheast2', 'EU', 'EUCentral1', 'SAEast', 'USWest', 'USWest2']:
#    inputLocation = 'Default'
#logging.info("Location = %s" % inputLocation)
#
#print ("Please input company CIK")
#cik = input()
#logging.info("CIK = %s" % cik)
#
#print ("Please input the accession number")
#accessionNumber = input()
#logging.info("accession number = %s" % accessionNumber)


config = ConfigParser()

config_file = os.path.join(os.path.dirname(__file__), 'config.ini')

config.read(config_file)
default=config['aws.data']
accessKey=default['accessKey']
secretAccessKey = default['secretAccessKey']
inputLocation = default['inputLocation']
cik = default['cik']
accessionNumber =default['accessionNumber']

if inputLocation not in ['APNortheast', 'APSoutheast', 'APSoutheast2', 'EU', 'EUCentral1', 'SAEast', 'USWest', 'USWest2']:
    inputLocation = 'Default'
    
logging.info("Access Key = %s" % accessKey)  
logging.info("Secret Access Key = %s" % secretAccessKey)
logging.info("Location = %s" % inputLocation)
logging.info("CIK = %s" % cik)
logging.info("accession number = %s" % accessionNumber)
 



for i in range(1,argLen):
    argument=sys.argv[i]
    if argument.startswith('cik='):
        pos=argument.index("=")
        cik=argument[pos+1:len(argument)]
        continue
    elif argument.startswith('accessionNumber='):
        pos=argument.index("=")
        accessionNumber=argument[pos+1:len(argument)]
        continue
    elif argument.startswith('accessKey='):
        pos=argument.index("=")
        accessKey=argument[pos+1:len(argument)]
        continue
    elif argument.startswith('secretKey='):
        pos=argument.index("=")
        secretAccessKey=argument[pos+1:len(argument)]
        continue
    elif argument.startswith('location='):
        pos=argument.index("=")
        inputLocation=argument[pos+1:len(argument)]
        continue

print("CIK=",cik)
print("Accession Number=",accessionNumber)
print("Access Key=",accessKey)
print("Secret Access Key=",secretAccessKey)
print("Location=",inputLocation)

############### Validate amazon keys ###############
if not accessKey or not secretAccessKey:
    logging.warning('Access Key and Secret Access Key not provided!!')
    print('Access Key and Secret Access Key not provided!!')
    exit()

AWS_ACCESS_KEY_ID = accessKey
AWS_SECRET_ACCESS_KEY = secretAccessKey

try:
    conn = boto.connect_s3(AWS_ACCESS_KEY_ID,
            AWS_SECRET_ACCESS_KEY)

    print("Connected to S3")

except:
    logging.info("Amazon keys are invalid!!")
    print("Amazon keys are invalid!!")
    exit()


############### Create the URL by inputed CIK and ACC_No ###############
url_1 = "https://www.sec.gov/Archives/edgar/data/"
if not cik or not accessionNumber:
    logging.warning('CIK or AccessionNumber was not mentioned, assuming the values to be 0000104169 and 0000104169-17-000081 respectively. This is original data of Walmart')
    cik='0000104169'
    accessionNumber = '0000104169-17-000081'
else:
    logging.info('CIK: %s and AccessionNumber: %s given', cik, accessionNumber)

final_cik = cik.lstrip("0")
Acc_no_without_hyfen = accessionNumber.replace("-","")
final_url = url_1 + final_cik + "/" + Acc_no_without_hyfen + "/" + accessionNumber + "-index.html"        
logging.info("URL generated is: "+ final_url)



url2=''
try:
    page = urllib.request.urlopen(final_url)
    soup = BeautifulSoup(page,"lxml") 
    form = soup.find(id='formName').get_text()
    formname = form[6:10]
    
	
    formtype = soup.findAll('td', text = formname)[0]
  
    all_links = soup.find_all('a')
    for link in all_links:
        href=link.get("href")
        if "10q.htm" in href:
            url2 = "https://www.sec.gov/" + href
            logging.info("Form's URL is: "+ url2)
            break;
        elif "Form 10-Q" in href:
            url2 = "https://www.sec.gov/" + href
            logging.info("Form's URL is: "+ url2)
            break;
            
        elif "10-q" in href:
            url2 = "https://www.sec.gov/" + href
            logging.info("Form's URL is: "+ url2)
            break; 
        else:
            url2=""
            
            
    if url2 is "":
        logging.info("Invalid URL!!!")
        print("Invalid URL!!!")
        exit()  
         
except urllib.error.HTTPError as err:
    logging.warning("Invalid CIK or AccNo")
    exit()



if not os.path.exists('Extracted_csvs'):
    os.makedirs('Extracted_csvs')
        
page = urllib.request.urlopen(url2)
soup = BeautifulSoup(page,"lxml")
all_tables = soup.select('div table')

#taking tables having selected files

refined_tables=[]

for tab in all_tables:
    for tr in tab.find_all('tr'):
        f=0
        for td in tr.findAll('td'):
            if('$' in td.get_text() or '%' in td.get_text()):
                refined_tables.append(tab)
                f=1;
                break;
        if(f==1):
            break;    

#unwanted tables

for tab in refined_tables:
    records = []
    for tr in tab.find_all('tr'):
        rowString=[]
        for td in tr.findAll('td'):
            p = td.find_all('p')
            if len(p)>0:
                for ps in p:
                    ps_text = ps.get_text().replace("\n"," ") 
                    ps_text = ps_text.replace("\xa0","")                 
                    rowString.append(ps_text)
            else:
                td_text=td.get_text().replace("\n"," ")
                td_text = td_text.replace("\xa0","")
                rowString.append(td_text)
        records.append(rowString)        
    with open(os.path.join('Extracted_csvs' , str(refined_tables.index(tab)) + 'Tables.csv'), 'w') as f:
        writer = csv.writer(f)
        writer.writerows(records)
            
logging.info('Tables successfully extracted to csv')
    
#creating zip for every available file
def zipdir(path, ziph, refined_tables):
   
    for tab in refined_tables:
        ziph.write(os.path.join('Extracted_csvs', str(refined_tables.index(tab))+'Tables.csv'))
    ziph.write(os.path.join('log_file_1.log'))   

zipf = zipfile.ZipFile('Log_File_1.zip', 'w', zipfile.ZIP_DEFLATED)
zipdir('/', zipf, refined_tables)
zipf.close()
logging.info('csv and log file zipped')



server_location=''

if inputLocation == 'APNortheast':
    server_location=boto.s3.connection.Location.APNortheast
elif inputLocation == 'APSoutheast':
    server_location=boto.s3.connection.Location.APSoutheast
elif inputLocation == 'APSoutheast2':
    server_location=boto.s3.connection.Location.APSoutheast2
elif inputLocation == 'CNNorth1':
    server_location=boto.s3.connection.Location.CNNorth1
elif inputLocation == 'EUCentral1':
    server_location=boto.s3.connection.Location.EUCentral1
elif inputLocation == 'EU':
    server_location=boto.s3.connection.Location.EU
elif inputLocation == 'SAEast':
    server_location=boto.s3.connection.Location.SAEast
elif inputLocation == 'USWest':
    server_location=boto.s3.connection.Location.USWest
elif inputLocation == 'USEast1':
    server_location=boto.s3.connection.Location.USEast1
try:   
    time_variable = time.time()
    timestamp_variable = datetime.datetime.fromtimestamp(time_variable)    
    bucket_name = AWS_ACCESS_KEY_ID.lower()+str(timestamp_variable).replace(" ", "").replace("-", "").replace(":","").replace(".","")
    bucket = conn.create_bucket(bucket_name, location=server_location)
    print("Bucket created")
    zipfile = 'Log_File_1.zip'
    print ("Uploading %s to Amazon S3 bucket %s", zipfile, bucket_name)

    def percent_cb(complete, total):
        sys.stdout.write('.')
        sys.stdout.flush()
    
    k = Key(bucket)
    k.key = 'Log_File_1'
    k.set_contents_from_filename(zipfile,
        cb=percent_cb, num_cb=10)
    print("Zip File successfully uploaded to S3")
    logging.info("Zip File successfully uploaded to S3")
except Exception as ex:
    print(ex)
    logging.info(ex)
    logging.info("Amazon keys are invalid!!")
    print("Amazon keys are invalid!!")
    exit()
