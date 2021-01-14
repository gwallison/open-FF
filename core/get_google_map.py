# -*- coding: utf-8 -*-
"""
Created on Sat Nov 21 08:13:36 2020

@author: Gary
used to fetch a map image with markers using staticmaps api

#### NOTE THIS HAS A LINK TO PASSWORDS AND API KEYS.  DO NOT POST!!!

"""
import pandas as pd
#from pyproj import Proj, transform
from pyproj import Transformer
#import requests
#import IPython.display as Disp

def getGoogleProjCoord(lat,lon,proj='WGS84'):
    # returns lat, lon in the projection that Google uses (WGS84), but
    #  you need to include the input projection
    if proj.upper() == 'WGS84':
        return lat,lon # no conversion needed
    transformer = Transformer.from_crs(proj.upper(), 'WGS84')
    olon,olat = transformer.transform(lon, lat)
    return olat,olon

# =============================================================================
# 
# def getGoogleProjCoord(lat,lon,proj='WGS84'):
#     # returns lat, lon in the projection that Google uses (WGS84), but
#     #  you need to include the input projection
#     if proj.upper() == 'WGS84':
#         return lat,lon # no conversion needed
#     inp = Proj(proj.upper())
#     outp = Proj('WGS84')
#     olon, olat = transform(inp,outp,lon,lat)
#     return olat,olon
# =============================================================================

def getMapAPI_key():
    d = pd.read_csv('c:/MyDocs/sandbox/data/master_context.csv')
    dic = d.to_dict(orient='list')
    out = {}
    for i,item in enumerate(dic['variable']):
        out[item]=dic['value'][i]
    return out['mapsAPI_password']

def getURL(locations=[(51.477222,0)],
           outside=[],other=[], size='640x640',maptype='satellite',
           zoomlevel=None,
           labels=['A','B','C','D','E','F','G','H','I','J','K','L','M',
                  'N','O','P','Q','R','S','T','U','V','W','X','Y','Z',
                  '0','1','2','3','4','5','6','7','8','9'],
           adjProjection=False):
    """ If adjProjection is True, location tuples need 3 values:
           (lat, lon, projection) and will be changed to fit Google's
           assumed projection """
    if adjProjection:
        lsts = [locations,outside,other]
        for lst in lsts:
            tmp = []
            for tup in lst:
                tmp.append((getGoogleProjCoord(tup[0], tup[1], tup[2])))
            lst = tmp
        
    s = f'https://maps.googleapis.com/maps/api/staticmap?size={size}'
    for i,tup in enumerate(locations):
        s+= f'&markers=color:red%7Clabel:{labels[i % len(labels)]}%7C'
        s+= f'{str(tup[0])},{str(tup[1])}'
    for i,tup in enumerate(outside):
        s+= f'&markers=color:blue%7Clabel:{labels[i % len(labels)]}%7C'
        s+= f'{str(tup[0])},{str(tup[1])}'
    for i,tup in enumerate(other):
        s+= f'&markers=color:gray%7Clabel:{labels[i % len(labels)]}%7C'
        s+= f'{str(tup[0])},{str(tup[1])}'
    #s+= f'&size={size}'
    s+= f'&maptype={maptype}'
    if zoomlevel!=None:
        s+= f'&zoom={zoomlevel}'
    s+= f'&key={getMapAPI_key()}'
    return s

def getMapLink(lat=51.477222,lon=0):
    return f'https://www.google.com/maps/@?api=1&map_action=map&center={lat},{lon}&basemap=satellite'

def getSearchLink(lat=51.477222,lon=0,proj='WGS84'):
    # note that by including the input Projection, the process may take MUCH longer.
    if proj!= 'WGS84':
        print(f'Projection = {proj}')
        lat,lon = getGoogleProjCoord(lat, lon, proj)
#    return f'https://www.google.com/maps/search/?api=1&query={lat},{lon}'
    return f'https://maps.google.com/maps?q={lat},{lon}&t=k'

def wrap_URL_in_html(link,txtToShow='MAP'):
    return f'<a href="{link}" target="_blank" >{txtToShow}</a>'

def wrap_URL_in_redirect(link,delay='0'):
    return f'<meta http-equiv="refresh" content="{delay}; URL={link}" />'

if __name__ == '__main__':
    print(getSearchLink(36.11578,-100.80145))
# =============================================================================
#     print(getURL(locations=[(36.11578,-100.80145),(36.11578,-100.80245)],
#                  outside=[(36.11478,-100.80245)]))
# 
# =============================================================================
