def get_zone_info(shpfile_path, longitude, latitude)->dict:
    """
    Input: 
        shpfile: shapefile path
        longitude: longitude of the point
        latitude: latitude of the point
    Output: dictionary zoneID as key, zoneName as value
        zoneID: 6-digit zone ID as in the shapefile
        zoneName: name of the zone as in the shapefile
    """
    from pyproj import Proj, transform, Transformer
    from shapely.geometry import Point, Polygon, shape
    import geopandas as gpd

    # read shapefile
    gdf = gpd.read_file(shpfile_path)
    
    # convert to WGS84
    new_gdf = gdf.to_crs("EPSG:4326", inplace=False)
    
    zoneInfo = {}
    # retrieve zoneID and zoneDescription, and ZoneName for a given point(Longitude, Latitude)
    for i in range(len(new_gdf)):
        polygon = new_gdf.loc[i,'geometry']
        if polygon.contains(Point(longitude, latitude)):
            zoneID = new_gdf.loc[i,'zoneID']
            zoneName = new_gdf.loc[i,'zoneName']
            zoneDescription = new_gdf.loc[i,'zoneDescription']
            zoneInfo['zoneID'] = [zoneName, zoneDescription]
            break
    
    return zoneInfo


