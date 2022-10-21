import os
import urllib.request
import zipfile
import shutil

class Crypto:
    def __init__(self,pair):
        self.pair = pair
    
    def make_month_to_url(self,month):
        if month < 10:
            url_month = "0{}".format(month)
        else:
            url_month = month
        return url_month

    def download_file(self,pair,interval,year,month,saving_dir):
        base_url = 'https://data.binance.vision'
        url_month = self.make_month_to_url(month)
        saved_file_name = "{}-{}-{}-{}.zip".format(pair.upper(),interval,year,url_month)
        request_url = "{}/data/spot/monthly/klines/{}/{}/{}".format(
                base_url,pair.upper(),interval,saved_file_name)
        print(request_url)        
        urllib.request.urlretrieve(request_url, "{}/{}".format(saving_dir,saved_file_name))

    def get_dataset(self):
        if self.start_year > self.end_year:
            print("Start year can not be larger than End year!")
        
        elif self.start_year == self.end_year:
            if self.start_month > self.end_month:
                print("Start month must be lower or equals to End month!")
            else:
                print("Downloading dataset in single file ...")
                for month in range(self.start_month, self.end_month+1):
                    self.download_file(self.pair,self.interval,self.end_year,month,self.saving_dir)
                print("Download completed!")

        else:
            print("Downloading dataset in single file ...")
            for year in range(self.start_year, self.end_year+1):
                if year == self.start_year:
                    for month in range(self.start_month, 13):
                        self.download_file(self.pair,self.interval,year,month,self.saving_dir)
                elif year < self.end_year:
                    for month in range(1,13):
                        self.download_file(self.pair,self.interval,year,month,self.saving_dir)
                else:
                    for month in range(1,self.end_month+1):
                        self.download_file(self.pair,self.interval,year,month,self.saving_dir)
            print("Download completed!")
    
    def extract_data(self):
        print("Start extracting ...")
        for file in sorted(os.listdir(self.saving_dir)):
            if file.endswith(".zip"):
                print("----- Extracting file: {}".format(file))
                file_to_unzip = "{}/{}".format(self.saving_dir,file)          
                with zipfile.ZipFile(file_to_unzip, 'r') as zip_file:
                    zip_file.extractall(path=self.saving_dir)        
        print("All files are extracted!")

    def merge_data(self):
        self.one_file_dir = '{}/one-file-{}-from-{}-{}-to-{}-{}-{}'.format(
                        self.saving_dir,self.pair,self.start_year,self.start_month,self.end_year,self.end_month,self.interval)
        if not os.path.isdir(self.one_file_dir):
            os.makedirs(self.one_file_dir, exist_ok=True)
        self.one_file_name = '{}/{}-from-{}-{}-to-{}-{}-{}.csv'.format(
                        self.one_file_dir,self.pair,self.start_year,self.start_month,self.end_year,self.end_month,self.interval)
        print("Start merge csv files into one ...")
        with open(self.one_file_name,'a') as f:
            for file in sorted(os.listdir(self.saving_dir)):
                if file.endswith(".csv"):
                    file_to_merge = "{}/{}".format(self.saving_dir,file)
                    with open(file_to_merge, "r") as merged_file:
                        data = merged_file.readlines()
                        f.writelines(data)

        print("Merge done!")

    def one_file_data(self,interval,start_year,start_month,end_year,end_month):
        self.interval = interval
        self.start_year = start_year
        self.start_month = start_month
        self.end_year = end_year
        self.end_month = end_month
        
        self.saving_dir = '{}/{}/{}-from-{}-{}-to-{}-{}-{}'.format(
                    os.getcwd(),self.pair,self.pair,self.start_year,self.start_month,self.end_year,self.end_month,self.interval)
        if not os.path.isdir(self.saving_dir):
            os.makedirs(self.saving_dir, exist_ok=True)
        
        self.get_dataset()
        self.extract_data()
        self.merge_data()

    def copy_file_to_all_data_folder(self,del_old = False):
        self.all_data_folder = './all_data'
        if not os.path.isdir(self.all_data_folder):
            os.makedirs(self.all_data_folder, exist_ok=True)
        source = self.one_file_name
        destination = "{}/{}-from-{}-{}-to-{}-{}-{}.csv".format(
                    self.all_data_folder,self.pair,self.start_year,self.start_month,self.end_year,self.end_month,self.interval)
        shutil.copyfile(source, destination)

        if del_old == True:
            shutil.rmtree('./{}'.format(self.pair))
        
        print("All done! Your dataset of {} from {}-{} to {}-{} in timeframe {} is now ready to use!".format(
                self.pair.upper(),self.start_year,self.start_month,self.end_year,self.end_month,self.interval))
        
        
