# Datasets

This folder is the working directory of the data pipeline. 

**No actual data should be committed to the repository.** 

## How to download data to run locally: 
1. Access the dataset of the [CTFB](http://fauna.jbrj.gov.br/). It can be accessed in [GBIF](https://www.gbif.org/dataset/811d48a6-fd04-4a34-81f2-7605492e54b8) 
2. Download DarwinCore Archive (DwC-A) file. 
3. Extract the zip file.
4. Copy only the 'taxon.txt' and 'vernacularname.txt' files into the folder. 
5. Run the script etl_insecta.py.