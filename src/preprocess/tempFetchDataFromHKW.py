import urllib2
import time

tm = str(int( time.time()* 1000))  # Current timestamp
station_grid_id = '0207' # You can find the grid id in the station_config.json file
path = 'http://www.hko.gov.hk/PDADATA/locspc/data/gridData/'+ station_grid_id+ '_en.xml?_=' + tm
request = urllib2.urlopen(path)
print request.read()
request.close()