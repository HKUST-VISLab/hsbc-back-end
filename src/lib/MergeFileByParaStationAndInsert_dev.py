
# coding: utf-8

# In[1]:


from library import *
import os
import re
import pandas as pd
from pandas import Series
from pandas import DataFrame
import json
from io import StringIO

client = MongoClient('127.0.0.1', 27017)

db = client['weather_hkust']
sub_hour_weather_collection_name = 'subhour_weather_hkust'
sub_hour_weather_collection = db[sub_hour_weather_collection_name]
subhour_weather_station = db['subhour_weather_station']


# In[50]:


class WeatherFileProcessor:
    """
    This is a class consists of multiple static functions to create, initialize database for the weather data from hkust.
    
    Collection(subhour_weather_hkust): store the weather data collected subhourly;
    Collection(subhour_weather_station): store the weather station, station code, station location;
    
    The weather with different parameter(wind, humudity, temperature) and from different stations are stored in different files,
    so this class first merge the data of same time and in same station together, and then store them into the database.  
    
    Further improvement, aggregate the data by time (every five minutes)
    """
    
    @classmethod
    def init_collection(self):
        """
        This is a function initialize the collection of weather records
        :param station_code: code of station, should be of string type, specified in forecast_station_config
        :return: forecast data of the site
        """
        indexs = []
        if sub_hour_weather_collection_name not in db.collection_names():
            sub_hour_weather_collection.create_index('time')
            sub_hour_weather_collection.create_index([("loc", pymongo.GEOSPHERE)])
        else:
            for index_agg in sub_hour_weather_collection.index_information():
                indexs.append(index_agg.split("_")[0])
            if 'time' not in indexs:
                sub_hour_weather_collection.create_index('time')
            if 'loc' not in indexs:
                sub_hour_weather_collection.create_index([("loc", pymongo.GEOSPHERE)])
        subhour_weather_station.create_index([("loc", pymongo.GEOSPHERE)])
        subhour_weather_station.create_index('station_code')
    
    @classmethod
    def extract_para_and_station(self, file_path):
        """
        Process the head of each file, extract the weather parameter, station name and station code;
        Example of the file(first three lines)
        "Wind (Sub Hour), lat=22.2011, lon=114.0267 [CCH_AWS]"
        "Station: CCH_AWS"
        "Time Period (UTC+8): 20170101-20170630"

        :param file_path: the path of the file
        :return: {
                    'weather': weather, 
                    'lat': lat, 'lon': lon, 
                    'station_code': station_code
                 }
        """
        with open(file_path) as input:
            # Read weather para, lon and lat from the first line
            line = input.readline()
            line = line.replace('"', '')
            line = re.sub("[\(\[].*?[\)\]]", "", line)
            segs = line.split(',')
            segs = [seg.strip() for seg in segs]
            weather = '_'.join(segs[0].lower().split(' '))
            lat = segs[1].split('=')[1]
            lon = segs[2].split('=')[1]

            # Read station from the second line
            line = input.readline()
            line = line.replace('"', '')
            segs = line.split(':')
            segs = [seg.strip() for seg in segs]
            station_code = segs[1]
            return {'weather': weather, 'lat': lat, 'lon': lon, 'station_code': station_code}
        
    @classmethod
    def generate_panda_dataframe(self, file_path):
        """
        Process process a file, generate pandas Dataframe and file paremeter;
        Example of the file(first three lines)
        

        :param file_path: the path of the file
        :return: df, {
                    'weather': weather, 
                    'lat': lat, 'lon': lon, 
                    'station_code': station_code
                 }
        """
        file_config = self.extract_para_and_station(file_path)
        with open(file_path) as input:

            df = pd.read_csv(StringIO(input.read()), skiprows=4, header=None)  
            if file_config['weather'] != 'wind':
                df = df[[0,2]]
                df.columns = ['time', file_config['weather']]
                df = df.assign(station_code = [file_config['station_code'] for _ in range(len(df))]) 

            else:
                df = df[[0,2,3]]
                df.columns = ['time','wind_speed', 'wind_direction']
                df = df.assign(station_code = [file_config['station_code'] for _ in range(len(df))])  
            return df, file_config
        
        
    @classmethod 
    def read_all_files(self):
        """
        Read all the files in the directory "../../data/weather_station_param" and the subdirectory
        
        :param file_path: the path of the file 
        :return: file list
        """
        import os
        file_list = []
        if __name__ == "__main__":
            data_folder = '../../data/weather_station_param'
        else:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            data_folder = os.path.join(current_dir, '../../data/weather_station_param')

        dirs = [f for f in os.listdir(data_folder) if os.path.isdir(os.path.join(data_folder, f))]
        files = []
        for dir in dirs:
            dir_path = os.path.join(data_folder, dir)
            filename_objs = [{'filename': f, 'sub_dir': dir} for f in os.listdir(dir_path) if
                             os.path.isfile(os.path.join(dir_path, f))]
            files += filename_objs

        for file_obj in files:
            filename = file_obj['filename']
            sub_dir = file_obj['sub_dir']
            file_list.append(data_folder + '/' + sub_dir + '/' + filename)

        return file_list
    
    @classmethod
    def init_subhour_station_conf_db(self):
        """
        Read all the files in the directory "../../data/weather_station_param" and the subdirectory;
        Initialize all the stations, the station configuration will be put at collection subhour_weather_station
        :param file_path: 
        :return
        """
        files = self.read_all_files()
        station_config_map = {}
        station_config_list = []
        for file in files:
            file_config = self.extract_para_and_station(file)

            station_code = file_config['station_code']
            if station_code not in station_config_map:
                station_config_map[station_code] = {
                    'station_code': station_code,
                    'loc': [float(file_config['lon']), float(file_config['lat'])]
                }
                station_config_list.append(station_config_map[station_code])

        subhour_weather_station.insert_many(station_config_list)
        subhour_weather_station.create_index([("loc", pymongo.GEOSPHERE)])
        subhour_weather_station.create_index('station_code')

      
    
    @classmethod
    def processing(self):
        """
        Read all the files in the directory "../../data/weather_station_param" and the subdirectory;
        Merge files and store them into the database;
        :param file_path: 
        :return
        """
        files = self.read_all_files()
        source_df = DataFrame()
        
        station_df_map = {}
        station_dfs_merge = {}
        station_config_map = {}
        deduplicate_dfs_map = {}
        
        for file in files:
            print(file.split('/')[-1])
            df, file_config = self.generate_panda_dataframe(file)
            station_code = file_config['station_code']
            station_config_map[station_code] = [float(file_config['lon']), float(file_config['lat'])]
            print(station_code)
            if station_code not in station_df_map:
                station_df_map[station_code] = []
            station_df_map[station_code].append(df)

        for station_code in station_df_map:
            dfs = station_df_map[station_code]
            print(station_code, len(dfs))
            source_df = DataFrame()
            start_time = time.time()
            for temp_df in dfs:
                temp_df = temp_df.drop_duplicates(subset = ['time', 'station_code'])
                if source_df.empty == True:
                    source_df = temp_df   
                else:
                    source_df = pd.merge(source_df, temp_df, how='outer', on=['time', 'station_code'], suffixes=('_c', '_c'))
                    temp_df = temp_df.drop_duplicates(subset = ['time', 'station_code'])
                print(time.time() - start_time)
            station_dfs_merge[station_code] = source_df
                
        
        for station_code in station_dfs_merge:
            agg_df = station_dfs_merge[station_code]

            # I don't understand    
            for c in agg_df.columns:
                if c[-2:] == '_c':
                    agg_df = agg_df.rename(columns = {c: c[:-2]})

            agg_df = agg_df.groupby(agg_df.columns, axis=1).max()            
            print(station_code, list(agg_df.columns))
            deduplicate_dfs_map[station_code] = agg_df
        
        for station_code in deduplicate_dfs_map:
            print('Processing', station_code, 'of', len(deduplicate_dfs_map))
            current_df = deduplicate_dfs_map[station_code]
            current_df['time'] = current_df['time'].apply(lambda t:  time.mktime(time.strptime(t, "%Y/%m/%d %H:%M:%S")) if type(t) == str else t)
            current_df = current_df.assign(loc = [station_config_map[station_code] for _ in range(len(current_df))]) 

            current_df_T = current_df.T
            current_df_T_dict = current_df_T.to_dict()
            dict_arr = []
            for key in current_df_T_dict:
                dict_arr.append(current_df_T_dict[key])
            sub_hour_weather_collection.insert_many(dict_arr)


if __name__ == "__main__":
    WeatherFileProcessor.init_collection()
    WeatherFileProcessor.init_subhour_station_conf_db()
    WeatherFileProcessor.processing()


