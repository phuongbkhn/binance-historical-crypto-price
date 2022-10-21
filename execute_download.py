from binance_dataset import Crypto

# pairs = ['btcbusd','ethbusd','bnbbusd','xrpbusd','adabusd','solbusd','dogebusd','dotbusd','shibbusd']
pairs = ['btcbusd','ethbusd']
intervals = ['15m','1h']
start_year = 2022
start_month = 1
end_year = 2022
end_month = 8
del_old = True

def execute_download(del_old):
    for pair in pairs:
        pair = Crypto(pair)
        for i in intervals:
            pair.one_file_data(i,start_year,start_month,end_year,end_month)
            pair.copy_file_to_all_data_folder(del_old)

if  __name__ == "__main__":
    print("Start downloading...")
    execute_download(del_old)
    print("FINISH!")