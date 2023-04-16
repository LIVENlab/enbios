''' Plot FAO data on world bmaps '''
import datetime
import os
import re
import tarfile

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.collections import PatchCollection
from matplotlib.patches import Polygon
from mpl_toolkits.basemap import shapefile

from magic_box import source_fao_restful

# Decompress Natural Earth boundaries shapefile
FDATA = os.path.dirname(os.path.abspath(__file__))
FSHP = '%s/ne_110m_admin_0_countries.shp' % FDATA

if not os.path.exists(FSHP):
    with tarfile.open(re.sub('shp$', 'tar.gz', FSHP)) as tar:
        for item in tar:
            tar.extract(item, FDATA)

def get_countries_shapefiledata():
    ''' Get country meta-data from Natural Earth shapefile
    see http://www.naturalearthdata.com/downloads/110m-cultural-vectors/

    Returns
    -----------
    data : pandas.core.series.Series
        Country Data

    Example
    -----------
    >>> df = faobmap.get_countries_shapefiledata()
    >>> df.shape
    (255, 65)
    '''
    shf = shapefile.Reader(re.sub('\\.shp', '', FSHP))

    cols = [f[0] for f in shf.fields[1:]]

    data = []
    for shprec in shf.shapeRecords():
        rec = {k:v for k, v in zip(cols, shprec.record)}
        data.append(rec)

    data = pd.DataFrame(data)

    # Fix missing ISO 3 country code
    idx = data['iso_a3'] == '-99'
    data.loc[idx, 'iso_a3'] = data.loc[idx, 'wb_a3']

    return data


def plot(bmap, data, cat, \
        country_field='country', \
        value_field='value', \
        cbmap=plt.get_cmap('Blues'), \
        ndigits=0):
    ''' Draw polygon shapefile.
        Arguments sent to PatchCollection constructor

    Parameters
    -----------
    bmap : mpl_toolkits.basebmap.Basebmap
        Axe to draw data on
    data : pandas.core.series.Series
        Data to be plotted
    cat : list
        Categories
    country_field : str
        Name of field in data containing ISO3 country codes
    value_field : str
        Name of field in data containing data to be plotted
    cbmap : matplotlib.colors.Colorbmap
        Color bmap to use for each category
    ndigits : int
        Number of digits to be displayed in legend

    Example
    -----------
    >>> import numpy as np
    >>> from hygis import oz
    >>> import matplotlib.pyplot as plt
    >>> nval = 200
    '''

    # Check inputs
    nbc = data.groupby(country_field).apply(len)
    if len(nbc[nbc > 1]) > 0:
        raise ValueError('Some countries are reported ' \
                'more than once in dataset')

    # Add country polygons to basebmap
    shapefilepath = os.path.basename(FSHP)
    bmap.readshapefile(re.sub('\\.shp$', '', FSHP), shapefilepath, \
            drawbounds=False)

    # Correct cat to account for min/max
    if cat[0] > data[value_field].min():
        cat = [data[value_field].min()] + cat

    if cat[-1] < data[value_field].max():
        cat = cat + [data[value_field].max()]

    # Build metadata dataframe
    cdata = get_countries_shapefiledata()
    cdata = cdata.rename(columns={'iso_a3':country_field})
    cdata = pd.merge(cdata, data, how='left', on=country_field)
    cdata['icat'] = pd.cut(cdata[value_field], cat, labels=False)
    cdata['icat'] = cdata['icat'].fillna(-1)

    # Initialise
    ncat = len(cat)-1
    colors = {i:cbmap(int(float(i)/ncat*cbmap.N)) for i in range(ncat)}
    colors[-1] = 'lightgrey'
    patches = {-1:[]}

    for i in range(ncat):
        patches[i] = []

    # Build patches
    for info, shape in zip(getattr(bmap, '%s_info' % shapefilepath), \
          getattr(bmap, shapefilepath)):

        iso = info['wb_a3']
        if iso == '-99':
            iso = info['iso_a3']

        idx = cdata[country_field] == iso
        if np.sum(idx) >= 1:
            icat = cdata.loc[idx, 'icat'].values[0]
            patches[icat].append(Polygon(np.array(shape), True))


    # plot patches with appropriate color
    for i in patches.keys():
        if len(patches[i]) > 0:
            # Add patch
            pac = PatchCollection(patches[i], \
                    edgecolor='none', facecolor=colors[i])
            bmap.ax.add_collection(pac)

            # Add legend entry
            if i == -1:
                label = 'No data'
            else:
                ndtxt = str(ndigits)
                label = ('%0.' + ndtxt + 'f to %0.' \
                            + ndtxt + 'f') % (cat[i], cat[i+1])
            bmap.ax.plot([], [], lw=10, color=colors[i], \
                    label=label)


def mapfooter(fig, database, dataset, field):
    ''' Add footer to a figure (code from wafari) '''

    now = datetime.datetime.now()
    label = 'source: FAO (%s, %s, %s)\nurl: %s\ngenerated: %s' % ( \
        database, dataset, field, \
        source_fao_restful.__fao_url__, \
        now.strftime('%d/%m/%Y %H:%M'))

    # Add label
    props = {
        'boxstyle':'round',
        'edgecolor':'none',
        'facecolor':'white'
    }

    fig.text(0.77, 0.03, label, \
            bbox=props, \
            color='#595959', \
            ha='left', fontsize=9)

