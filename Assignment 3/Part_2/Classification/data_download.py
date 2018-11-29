import requests
import re
import os
from bs4 import BeautifulSoup
from urllib.request import urlopen
from zipfile import ZipFile
from io import BytesIO
import time
import datetime
import sys
from tqdm import tqdm
import pandas as pd
import numpy as np
import glob
import csv
from configparser import ConfigParser

url = 'https://freddiemac.embs.com/FLoan/secure/auth.php'
postUrl = 'https://freddiemac.embs.com/FLoan/Data/download.php'


def payloadCreation(user, passwd):
    creds = {'username': user, 'password': passwd}
    return creds


def assure_path_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)


def extracrtZip(s, monthlistdata, path):
    abc = tqdm(monthlistdata)
    for month in abc:
        abc.set_description("Downloading %s" % month)
        r = s.get(month)
        z = ZipFile(BytesIO(r.content))
        z.extractall(path)


def getFilesFromFreddieMacPerQuarter(payload, quarter, testquarter):
    with requests.Session() as s:
        preUrl = s.post(url, data=payload)
        payload2 = {'accept': 'Yes', 'acceptSubmit': 'Continue', 'action': 'acceptTandC'}
        finalUrl = s.post(postUrl, payload2)
        linkhtml = finalUrl.text
        allzipfiles = BeautifulSoup(linkhtml, "html.parser")
        ziplist = allzipfiles.find_all('td')
        sampledata = []
        historicaldata = []
        count = 0
        # q =quarter[2:6]
        # t =testquarter[2:6]
        for li in ziplist:
            zipatags = li.findAll('a')
            for zipa in zipatags:
                fetchFile = 'historical_data1_'
                if (quarter in zipa.text):
                    if (fetchFile in zipa.text):
                        link = zipa.get('href')
                        foldername = 'HistoricalInputFiles'
                        Historicalpath = str(os.getcwd()) + "/" + foldername
                        assure_path_exists(Historicalpath)
                        finallink = 'https://freddiemac.embs.com/FLoan/Data/' + link
                        print(finallink)
                        historicaldata.append(finallink)
                elif (testquarter in zipa.text):
                    if (fetchFile in zipa.text):
                        link = zipa.get('href')
                        foldername = 'HistoricalInputFiles'
                        Historicalpath = str(os.getcwd()) + "/" + foldername
                        assure_path_exists(Historicalpath)
                        finallink = 'https://freddiemac.embs.com/FLoan/Data/' + link
                        print(finallink)
                        historicaldata.append(finallink)
        extracrtZip(s, historicaldata, Historicalpath)


def getFilesFromFreddieMac(payload, st, en):
    with requests.Session() as s:
        preUrl = s.post(url, data=payload)
        payload2 = {'accept': 'Yes', 'acceptSubmit': 'Continue', 'action': 'acceptTandC'}
        finalUrl = s.post(postUrl, payload2)
        linkhtml = finalUrl.text
        allzipfiles = BeautifulSoup(linkhtml, "html.parser")
        ziplist = allzipfiles.find_all('td')
        sampledata = []
        historicaldata = []
        count = 0
        slist = []
        for i in range(int(st), int(en) + 1):
            # print(i)
            slist.append(i)
        for li in ziplist:
            zipatags = li.findAll('a')
            for zipa in zipatags:
                for yr in slist:
                    if str(yr) in zipa.text:
                        if re.match('sample', zipa.text):
                            link = zipa.get('href')
                            foldername = 'SampleInputFiles'
                            Samplepath = str(os.getcwd()) + "/" + foldername
                            assure_path_exists(Samplepath)
                            finallink = 'https://freddiemac.embs.com/FLoan/Data/' + link
                            sampledata.append(finallink)
        extracrtZip(s, sampledata, Samplepath)

def  getTrainData(trainQ):
    print("Staring train data download")
    downloadPath='./HistoricalInputFiles/historical_data1_time_'+trainQ+'.txt'

    c_size = 1500000
    df = pd.read_csv('./head.txt', sep="|",
                     names=['LOAN_SEQ_NO', 'MONTHLY_REPORTING_PERIOD', 'CURRENT_ACTUAL_UPB', 'CURR_LOAN_DEL_STATUS',
                            'LOAN_AGE', 'REM_MTH_LEGAL_MATURITY', 'REPURCHASE_FLAG', 'MODIFICATION_FLAG',
                            'ZERO_BALANCE_CODE', 'ZERO_BALANCE_EFF_DATE', 'CURRENT_INTEREST_DATE',
                            'CURRENT_DEFERRED_UPB', 'DUE_DATE_LAST_PAID_INST', 'MI_RECOVERIES',
                            'NET_SALES_PROCEEDS',
                            'NON_MI_RECOVERIES', 'EXPENSES', 'LEGAL_COSTS', 'MAIN_PRES_COSTS',
                            'TAXES_INSURANCE', 'MISC_EXPENSES', 'ACTUAL_LOSS', 'MODIFICATION_COST', 'STEP_MOD_FLAG',
                            'DEFERRED_PAYMENT_MODI', 'EST_LOAN_TO_VALUE'],
                     skipinitialspace=True, error_bad_lines=False, index_col=False, dtype='unicode')

    for gm_chunk in pd.read_csv(downloadPath, sep="|",
                                names=['LOAN_SEQ_NO', 'MONTHLY_REPORTING_PERIOD', 'CURRENT_ACTUAL_UPB',
                                       'CURR_LOAN_DEL_STATUS',
                                       'LOAN_AGE', 'REM_MTH_LEGAL_MATURITY', 'REPURCHASE_FLAG', 'MODIFICATION_FLAG',
                                       'ZERO_BALANCE_CODE', 'ZERO_BALANCE_EFF_DATE', 'CURRENT_INTEREST_DATE',
                                       'CURRENT_DEFERRED_UPB', 'DUE_DATE_LAST_PAID_INST', 'MI_RECOVERIES',
                                       'NET_SALES_PROCEEDS',
                                       'NON_MI_RECOVERIES', 'EXPENSES', 'LEGAL_COSTS', 'MAIN_PRES_COSTS',
                                       'TAXES_INSURANCE', 'MISC_EXPENSES', 'ACTUAL_LOSS', 'MODIFICATION_COST',
                                       'STEP_MOD_FLAG',
                                       'DEFERRED_PAYMENT_MODI', 'EST_LOAN_TO_VALUE'],
                                skipinitialspace=True, error_bad_lines=False, index_col=False, dtype='unicode',
                                chunksize=c_size):
        frames = [df, gm_chunk]
        df = pd.concat(frames)
        print(df.shape)

    df.head()
    df.describe()
    print("done train data download")

    df = df.drop(columns=['REPURCHASE_FLAG', 'MODIFICATION_FLAG', 'ZERO_BALANCE_CODE', 'ZERO_BALANCE_EFF_DATE',
                          'DUE_DATE_LAST_PAID_INST', 'MI_RECOVERIES',
                          'NET_SALES_PROCEEDS', 'NON_MI_RECOVERIES', 'EXPENSES', 'LEGAL_COSTS', 'MAIN_PRES_COSTS',
                          'TAXES_INSURANCE',
                          'MISC_EXPENSES', 'ACTUAL_LOSS', 'MODIFICATION_COST', 'STEP_MOD_FLAG', 'DEFERRED_PAYMENT_MODI',
                          'EST_LOAN_TO_VALUE'])
    print("Staring train data cleansing")

    df.CURRENT_ACTUAL_UPB = df.CURRENT_ACTUAL_UPB.astype('float64')
    df.CURRENT_DEFERRED_UPB = df.CURRENT_DEFERRED_UPB.astype('float64')
    df.CURRENT_INTEREST_DATE = df.CURRENT_INTEREST_DATE.astype('float64')
    df[['MONTHLY_REPORTING_PERIOD', 'LOAN_AGE', 'REM_MTH_LEGAL_MATURITY']] = df[
        ['MONTHLY_REPORTING_PERIOD', 'LOAN_AGE', 'REM_MTH_LEGAL_MATURITY']].astype('int64')
    df[['LOAN_SEQ_NO', 'CURR_LOAN_DEL_STATUS']] = df[['LOAN_SEQ_NO', 'CURR_LOAN_DEL_STATUS']].astype('str')
    df['CURR_LOAN_DEL_STATUS'] = [999 if x == 'R' else x for x in (df['CURR_LOAN_DEL_STATUS'].apply(lambda x: x))]
    df['CURR_LOAN_DEL_STATUS'] = [0 if x == 'XX' else x for x in (df['CURR_LOAN_DEL_STATUS'].apply(lambda x: x))]

    df['YEAR'] = ['19' + x if x == '99' else '20' + x for x in (df['LOAN_SEQ_NO'].apply(lambda x: x[2:4]))]
    df['QUARTER'] = df['LOAN_SEQ_NO'].apply(lambda x: x[4:6])

    print("done train data cleanising")

    df[['CURR_LOAN_DEL_STATUS']] = df[['CURR_LOAN_DEL_STATUS']].astype('int64')
    df['DELINQUENT'] = (df.CURR_LOAN_DEL_STATUS > 0).astype(int)
    # df.drop('max_curr_ln_delin_status', axis = 1,inplace=True)
    # df.drop('CURR_LOAN_DEL_STATUS', axis = 1,inplace=True)

    df.drop('CURR_LOAN_DEL_STATUS', axis=1, inplace=True)

    traincols = ['MONTHLY_REPORTING_PERIOD', 'CURRENT_ACTUAL_UPB', 'LOAN_AGE',
                 'REM_MTH_LEGAL_MATURITY', 'CURRENT_INTEREST_DATE', 'CURRENT_DEFERRED_UPB']
    y_train = df['DELINQUENT']
    print("exporing train data to csv")

    Train_DF = df[traincols]

    Train_DF.to_csv('trainData_' + trainQ + '.csv')
    print("train data downloaded at- "+ 'trainData_' + trainQ + '.csv')


def getTestData(testQ):
    print("Staring test data download")
    downloadPath = './HistoricalInputFiles/historical_data1_time_' + testQ + '.txt'

    c_size = 1500000
    test_df = pd.read_csv('./head.txt', sep="|",
                     names=['LOAN_SEQ_NO', 'MONTHLY_REPORTING_PERIOD', 'CURRENT_ACTUAL_UPB', 'CURR_LOAN_DEL_STATUS',
                            'LOAN_AGE', 'REM_MTH_LEGAL_MATURITY', 'REPURCHASE_FLAG', 'MODIFICATION_FLAG',
                            'ZERO_BALANCE_CODE', 'ZERO_BALANCE_EFF_DATE', 'CURRENT_INTEREST_DATE',
                            'CURRENT_DEFERRED_UPB', 'DUE_DATE_LAST_PAID_INST', 'MI_RECOVERIES',
                            'NET_SALES_PROCEEDS',
                            'NON_MI_RECOVERIES', 'EXPENSES', 'LEGAL_COSTS', 'MAIN_PRES_COSTS',
                            'TAXES_INSURANCE', 'MISC_EXPENSES', 'ACTUAL_LOSS', 'MODIFICATION_COST', 'STEP_MOD_FLAG',
                            'DEFERRED_PAYMENT_MODI', 'EST_LOAN_TO_VALUE'],
                     skipinitialspace=True, error_bad_lines=False, index_col=False, dtype='unicode')

    for gm_chunk in pd.read_csv(downloadPath, sep="|",
                                names=['LOAN_SEQ_NO', 'MONTHLY_REPORTING_PERIOD', 'CURRENT_ACTUAL_UPB',
                                       'CURR_LOAN_DEL_STATUS',
                                       'LOAN_AGE', 'REM_MTH_LEGAL_MATURITY', 'REPURCHASE_FLAG', 'MODIFICATION_FLAG',
                                       'ZERO_BALANCE_CODE', 'ZERO_BALANCE_EFF_DATE', 'CURRENT_INTEREST_DATE',
                                       'CURRENT_DEFERRED_UPB', 'DUE_DATE_LAST_PAID_INST', 'MI_RECOVERIES',
                                       'NET_SALES_PROCEEDS',
                                       'NON_MI_RECOVERIES', 'EXPENSES', 'LEGAL_COSTS', 'MAIN_PRES_COSTS',
                                       'TAXES_INSURANCE', 'MISC_EXPENSES', 'ACTUAL_LOSS', 'MODIFICATION_COST',
                                       'STEP_MOD_FLAG',
                                       'DEFERRED_PAYMENT_MODI', 'EST_LOAN_TO_VALUE'],
                                skipinitialspace=True, error_bad_lines=False, index_col=False, dtype='unicode',
                                chunksize=c_size):
        frames = [test_df, gm_chunk]
        test_df = pd.concat(frames)
        print(test_df.shape)


    test_df.head()

    print("done train data download")

    test_df = test_df.drop(
        columns=['REPURCHASE_FLAG', 'MODIFICATION_FLAG', 'ZERO_BALANCE_CODE', 'ZERO_BALANCE_EFF_DATE',
                 'DUE_DATE_LAST_PAID_INST', 'MI_RECOVERIES',
                 'NET_SALES_PROCEEDS', 'NON_MI_RECOVERIES', 'EXPENSES', 'LEGAL_COSTS', 'MAIN_PRES_COSTS',
                 'TAXES_INSURANCE',
                 'MISC_EXPENSES', 'ACTUAL_LOSS', 'MODIFICATION_COST', 'STEP_MOD_FLAG', 'DEFERRED_PAYMENT_MODI',
                 'EST_LOAN_TO_VALUE'])
    print("Staring test data cleansing")

    test_df.CURRENT_ACTUAL_UPB = test_df.CURRENT_ACTUAL_UPB.astype('float64')
    test_df.CURRENT_DEFERRED_UPB = test_df.CURRENT_DEFERRED_UPB.astype('float64')
    test_df.CURRENT_INTEREST_DATE = test_df.CURRENT_INTEREST_DATE.astype('float64')
    test_df[['MONTHLY_REPORTING_PERIOD', 'LOAN_AGE', 'REM_MTH_LEGAL_MATURITY']] = test_df[
        ['MONTHLY_REPORTING_PERIOD', 'LOAN_AGE',
         'REM_MTH_LEGAL_MATURITY']].astype('int64')

    test_df[['LOAN_SEQ_NO', 'CURR_LOAN_DEL_STATUS']] = test_df[['LOAN_SEQ_NO', 'CURR_LOAN_DEL_STATUS']].astype('str')
    test_df['CURR_LOAN_DEL_STATUS'] = [999 if x == 'R' else x for x in
                                       (test_df['CURR_LOAN_DEL_STATUS'].apply(lambda x: x))]
    test_df['CURR_LOAN_DEL_STATUS'] = [0 if x == 'XX' else x for x in
                                       (test_df['CURR_LOAN_DEL_STATUS'].apply(lambda x: x))]

    test_df['YEAR'] = ['19' + x if x == '99' else '20' + x for x in (test_df['LOAN_SEQ_NO'].apply(lambda x: x[2:4]))]
    test_df['QUARTER'] = test_df['LOAN_SEQ_NO'].apply(lambda x: x[4:6])

    print("done test data cleanising")

    test_df[['CURR_LOAN_DEL_STATUS']] = test_df[['CURR_LOAN_DEL_STATUS']].astype('int64')
    test_df['DELINQUENT'] = (test_df.CURR_LOAN_DEL_STATUS > 0).astype(int)
    # df.drop('max_curr_ln_delin_status', axis = 1,inplace=True)
    # df.drop('CURR_LOAN_DEL_STATUS', axis = 1,inplace=True)

    test_df.drop('CURR_LOAN_DEL_STATUS', axis=1, inplace=True)

    testcols = ['MONTHLY_REPORTING_PERIOD', 'CURRENT_ACTUAL_UPB', 'LOAN_AGE',
                'REM_MTH_LEGAL_MATURITY', 'CURRENT_INTEREST_DATE', 'CURRENT_DEFERRED_UPB']
    y_test = test_df['DELINQUENT']
    Test_DF = test_df[testcols]
    print("exporing test data to csv")

    Test_DF.to_csv('testData_' + testQ + '.csv')
    print("test data downloaded at- "+ 'testData_' + testQ + '.csv')




def main():
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d_%H%M%S')
    args = sys.argv[1:]
    print("Starting")

    config = ConfigParser()

    config_file = os.path.join(os.path.dirname(__file__), 'config.ini')
    config.read(config_file)
    default = config['user.data']
    user=default['username']
    passwd=default['password']
    trainQ=default['trainQtr']
    testQ=default['testQtr']

    print("USERNAME=" + user)
    print("PASSWORD=" + passwd)
    print("TRAINQUARTER=" + (trainQ))
    print("TESTQUARTER=" + (testQ))

    payload = payloadCreation(user, passwd)
    getFilesFromFreddieMacPerQuarter(payload, trainQ, testQ)


    getTrainData(trainQ)
    getTestData(testQ)

    rx = []
    rx.append(trainQ)
    rx.append(testQ)

    with open("input.csv", "w") as resultFile:
        wr = csv.writer(resultFile, quoting=csv.QUOTE_NONE)
        wr.writerow(rx)


if __name__ == '__main__':
    main()