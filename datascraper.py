import urllib.request

def download(url, filename):
    '''download(url, filename)
    
    This function downloads a file from a specific URL and saves it to a determined filename.
    
    Input:
        - url: string with the URL of the file to download
        - filename: string with the name of the file to save the downloaded file
   
   Output: None 
    '''
    try:
        urllib.request.urlretrieve(url, filename)
        print(f"Downloaded {filename}")
    except Exception as e:
        print(f"Error downloading {url}: {e}. File does not exist")
    

def explore_download(start, end, output_folder, estado = 'sin'):
    ''' explore_download(start, end, output_folder)
        This function explores every url in a given range and saves the existing .txt files into an output folder

        Input:
            - start: a number associated with a file
            - end: final number to search in a range
            - output_folder: string with the name of the folder where the found files will be saved
            - estado: string with the state code (default is 'sin')
    '''
    for i in range(start, end):
        url = f"https://smn.conagua.gob.mx/tools/RESOURCES/Normales_Climatologicas/Diarios/{estado}/dia{i}.txt"
        filename = f"{output_folder}/{i}.txt"
        download(url, filename)

def bulk_download(stations, output_folder, estado = 'sin'):
    ''' bulk_download(output_folder)
    
    This function downloads a range of files from the SMN website.
    
    Input:
        - output_folder: string with the name of the folder where the files will be saved
        - stations: list of strings with the code of the stations to download
        - estado: string with the state code (default is 'sin')
        
    Output: None
    '''

    for estacion in stations:
        url = f"https://smn.conagua.gob.mx/tools/RESOURCES/Normales_Climatologicas/Diarios/{estado}/dia{estacion}.txt"
        filename = f"{output_folder}/{estacion}.txt"
        download(url, filename)