import pandas as pd
import segyio
import pandas as pd
import segyio
from tqdm import tqdm
import numpy as np

def get_sps_first_row(file):
    with open(file) as f:
        l = f.readlines()
        for i in range(200):
            if l[i][0].upper() == file[-1] or l[i][0].upper() == file[-1].upper():
                return i


def sps2df(version, x_file_path, r_file_path, s_file_path):
    x_names = ['X', '野外记录号，盘号', '野外记录号', '野外记录增量', '仪器代码', '炮点线号', '炮点点号',
               '点索引', '起始通道号', '终止通道号', '通道增量', '接收点线号', '起始接收点点号', '终止接收点点号', '接收点索引']
    r_names = ['R', '接收点线号', '接收点点号', '接收点索引', '接收点代码', '静校正量',
               '接收点深度', '基准面', '接收点X坐标', '接收点Y坐标', '接收点高程', '日期', '时分秒']
    s_names = ['S', '炮点线号', '炮点点号', '炮点索引', '炮点代码', '静校正量', '炮点深度',
               '基准面', '井口时间', '炮点水面深度', '炮点X坐标', '炮点Y坐标', '炮点高程', '日期', '时分秒']
    if version == 2006:
        x_file_colspecs = [(0, 1), (1, 7), (7, 15), (15, 16), (16, 17), (17, 27), (27, 37),
                           (37, 38), (38, 43), (43, 48), (48, 49), (49, 59), (59, 69), (69, 79), (79, 80)]
        r_file_colspecs = [(0, 1), (1, 11), (11, 21), (21, 24), (24, 26), (26, 30), (
            30, 34), (34, 38), (46, 55), (55, 65), (65, 71), (71, 74), (74, 80)]
        s_file_colspecs = [(0, 1), (1, 11), (11, 21), (23, 24), (24, 26), (26, 30), (30, 34), (
            34, 38), (38, 40), (40, 46), (46, 55), (55, 65), (65, 71), (71, 74), (74, 80)]

    if version == 1998:
        x_file_colspecs = [(0, 1), (1, 7), (7, 11), (11, 12), (12, 13), (13, 29), (29, 37), (
            37, 38), (38, 42), (42, 46), (46, 47), (47, 63), (63, 71), (71, 79), (79, 80)]
        r_file_colspecs = [(0, 1), (1, 17), (17, 25), (25, 26), (26, 28), (28, 32), (
            32, 36), (36, 40), (46, 55), (55, 65), (65, 71), (71, 74), (74, 80)]
        s_file_colspecs = [(0, 1), (1, 17), (17, 25), (25, 26), (26, 28), (28, 32), (32, 36), (
            36, 40), (40, 42), (42, 46), (46, 55), (55, 65), (65, 71), (71, 74), (74, 80)]
    x = pd.read_fwf(x_file_path, colspecs=x_file_colspecs,
                    names=x_names, index_col=None, skiprows=get_sps_first_row(x_file_path))
    r = pd.read_fwf(r_file_path, colspecs=r_file_colspecs,
                    names=r_names, index_col=None, skiprows=get_sps_first_row(r_file_path))
    s = pd.read_fwf(s_file_path, colspecs=s_file_colspecs,
                    names=s_names, index_col=None, skiprows=get_sps_first_row(s_file_path))
    return x, r, s


def get_sample_shot_receivers(x, r, s, 野外记录号):
    """画一炮所有接收点的位置"""
    野外记录号 = 野外记录号
    检波点 = pd.DataFrame()
    sample = x[x['野外记录号'] == 野外记录号]
    sample_s = sample.iloc[0,:]
    shot_pos_xy = s.loc[(s['炮点线号']==sample_s['炮点线号']) & (s['炮点点号']==sample_s['炮点点号']),['炮点X坐标','炮点Y坐标']]
    data_list = []
    for index, _ in sample.iterrows():
        炮点线号 = sample.loc[index, '炮点线号']
        炮点点号 = sample.loc[index, '炮点点号']
        起始通道号 = sample.loc[index, '起始通道号']
        终止通道号 = sample.loc[index, '终止通道号']
        通道增量 = sample.loc[index, '通道增量']
        接收点线号 = sample.loc[index, '接收点线号']
        起始接收点点号 = sample.loc[index, '起始接收点点号']
        终止接收点点号 = sample.loc[index, '终止接收点点号']
        检波点_tmp = r[(r['接收点线号'] == 接收点线号) & (
            r['接收点点号'] >= 起始接收点点号) & (r['接收点点号'] <= 终止接收点点号)]
        """此处未处理检波点增量、通道增量"""
        data_list.append(检波点_tmp)
        # print(len(检波点_tmp))
    return pd.concat(data_list, axis=0, ignore_index=True), shot_pos_xy

def get_one_shot_receiver_inSingleRfileLine(野外记录号, 通道号, x, r, s):
    """画野外记录号这一炮所有接收点的位置"""
    # 野外记录号 = 野外记录号_all_unique[100]
    # 通道号=1000
    sample_x = x[x['野外记录号'] == 野外记录号]
    sample_s = sample_x.iloc[0, :]
    data_list = []
    for index, _ in sample_x.iterrows():
        炮点线号 = sample_x.loc[index, '炮点线号']
        炮点点号 = sample_x.loc[index, '炮点点号']
        起始通道号 = sample_x.loc[index, '起始通道号']
        终止通道号 = sample_x.loc[index, '终止通道号']
        通道增量 = sample_x.loc[index, '通道增量']
        接收点线号 = sample_x.loc[index, '接收点线号']
        起始接收点点号 = sample_x.loc[index, '起始接收点点号']
        终止接收点点号 = sample_x.loc[index, '终止接收点点号']
        检波点_tmp = r[(r['接收点线号'] == 接收点线号) & (
            r['接收点点号'] >= 起始接收点点号) & (r['接收点点号'] <= 终止接收点点号)]
        """此处未处理检波点增量、通道增量"""
        data_list.append(检波点_tmp)
        # print(len(检波点_tmp))
        # shot_pos_xy = s.loc[(s['炮点线号']==sample_s['炮点线号']) & (s['炮点点号']==sample_s['炮点点号']),['炮点X坐标','炮点Y坐标']]
    炮点 = s.loc[(s['炮点线号'] == sample_s['炮点线号']) &
               (s['炮点点号'] == sample_s['炮点点号'])]
    检波点_all = pd.concat(data_list, axis=0, ignore_index=True)
    检波点_xfile = x.loc[(x['野外记录号'] == 野外记录号) & (
        x['起始通道号'] <= 通道号) & (x['终止通道号'] >= 通道号)]
    # print(检波点_xfile.head(10))
    检波点 = r.loc[(r['接收点线号'] == int(检波点_xfile['接收点线号'])) & (
        r['接收点点号'] == (通道号-int(检波点_xfile['起始通道号'])+int(检波点_xfile['起始接收点点号'])))]
    # return 检波点_xfile,炮点
    return 检波点, 炮点


def SegyHeader2DataFrame(segyfile):
    header_columns = ['x[9]' ,'x[13]','x[17]','x[21]','x[25]','x[29]','x[31]','x[33]','x[37]','x[41]','x[45]','x[49]','x[53]','x[57]','x[61]','x[65]',
'x[69]','x[71]','x[73]','x[77]','x[81]','x[85]','x[89]','x[91]','x[93]','x[95]','x[97]','x[99]','x[101]','x[103]','x[105]','x[107]','x[109]','x[111]',
'x[113]','x[117]','x[119]','x[121]','x[123]','x[125]','x[127]','x[129]','x[131]','x[133]','x[135]','x[137]','x[139]','x[141]','x[143]','x[145]','x[147]',
'x[149]','x[151]','x[153]','x[155]','x[157]','x[159]','x[161]','x[163]','x[165]','x[167]','x[169]','x[171]','x[173]','x[175]','x[177]','x[179]','x[181]',
'x[185]','x[189]','x[193]','x[197]','x[201]','x[203]','x[205]','x[211]','x[213]','x[215]','x[217]','x[219]','x[225]','x[231]']
    sgy_header = pd.DataFrame(columns=header_columns)
    with segyio.open(segyfile,'r+',ignore_geometry=True) as seis_data:
        seis_data.mmap()
        sgy_header = pd.DataFrame(columns=header_columns,index=[i for i in range(len(seis_data.header))])
        for index, x in tqdm(enumerate(seis_data.header[:])):
                sgy_header.loc[index,'x[1]'] = x[1]
                sgy_header.loc[index,'x[5]'] = x[5]
                sgy_header.loc[index,'x[9]'] = x[9]
                sgy_header.loc[index,'x[13]'] = x[13]
                sgy_header.loc[index,'x[17]'] = x[17]
                sgy_header.loc[index,'x[21]'] = x[21]
                sgy_header.loc[index,'x[25]'] = x[25]
                sgy_header.loc[index,'x[29]'] = x[29]
                sgy_header.loc[index,'x[31]'] = x[31]
                sgy_header.loc[index,'x[33]'] = x[33]
                sgy_header.loc[index,'x[37]'] = x[37]
                sgy_header.loc[index,'x[41]'] = x[41]
                sgy_header.loc[index,'x[45]'] = x[45]      
                sgy_header.loc[index,'x[49]'] = x[49]
                sgy_header.loc[index,'x[53]'] = x[53]
                sgy_header.loc[index,'x[57]'] = x[57]
                sgy_header.loc[index,'x[61]'] = x[61]
                sgy_header.loc[index,'x[65]'] = x[65]
                sgy_header.loc[index,'x[69]'] = x[69]
                sgy_header.loc[index,'x[71]'] = x[71]
                sgy_header.loc[index,'x[73]'] = x[73]
                sgy_header.loc[index,'x[77]'] = x[77]
                sgy_header.loc[index,'x[81]'] = x[81]
                sgy_header.loc[index,'x[85]'] = x[85]
                sgy_header.loc[index,'x[89]'] = x[89]
                sgy_header.loc[index,'x[91]'] = x[91]
                sgy_header.loc[index,'x[93]'] = x[93]
                sgy_header.loc[index,'x[95]'] = x[95]
                sgy_header.loc[index,'x[97]'] = x[97]
                sgy_header.loc[index,'x[99]'] = x[99]
                sgy_header.loc[index,'x[101]'] = x[101]
                sgy_header.loc[index,'x[103]'] = x[103]
                sgy_header.loc[index,'x[105]'] = x[105]
                sgy_header.loc[index,'x[107]'] = x[107]
                sgy_header.loc[index,'x[109]'] = x[109]
                sgy_header.loc[index,'x[113]'] = x[113]
                sgy_header.loc[index,'x[117]'] = x[117]
                sgy_header.loc[index,'x[119]'] = x[119]
                sgy_header.loc[index,'x[121]'] = x[121]
                sgy_header.loc[index,'x[123]'] = x[123]
                sgy_header.loc[index,'x[125]'] = x[125]
                sgy_header.loc[index,'x[127]'] = x[127]
                sgy_header.loc[index,'x[129]'] = x[129]
                sgy_header.loc[index,'x[131]'] = x[131]
                sgy_header.loc[index,'x[133]'] = x[133]
                sgy_header.loc[index,'x[135]'] = x[135]
                sgy_header.loc[index,'x[137]'] = x[137]
                sgy_header.loc[index,'x[139]'] = x[139]
                sgy_header.loc[index,'x[141]'] = x[141]
                sgy_header.loc[index,'x[143]'] = x[143]
                sgy_header.loc[index,'x[145]'] = x[145]
                sgy_header.loc[index,'x[147]'] = x[147]
                sgy_header.loc[index,'x[149]'] = x[149]
                sgy_header.loc[index,'x[151]'] = x[151]
                sgy_header.loc[index,'x[153]'] = x[153]
                sgy_header.loc[index,'x[155]'] = x[155]
                sgy_header.loc[index,'x[157]'] = x[157]
                sgy_header.loc[index,'x[159]'] = x[159]
                sgy_header.loc[index,'x[161]'] = x[161]     
                sgy_header.loc[index,'x[163]'] = x[163]
                sgy_header.loc[index,'x[165]'] = x[165]
                sgy_header.loc[index,'x[167]'] = x[167]
                sgy_header.loc[index,'x[169]'] = x[169]
                sgy_header.loc[index,'x[171]'] = x[171]
                sgy_header.loc[index,'x[173]'] = x[173]
                sgy_header.loc[index,'x[175]'] = x[175]
                sgy_header.loc[index,'x[177]'] = x[177]
                sgy_header.loc[index,'x[179]'] = x[179]
                sgy_header.loc[index,'x[181]'] = x[181]
                sgy_header.loc[index,'x[185]'] = x[185]             
                sgy_header.loc[index,'x[189]'] = x[189]
                sgy_header.loc[index,'x[193]'] = x[193]
                sgy_header.loc[index,'x[197]'] = x[197]
                sgy_header.loc[index,'x[201]'] = x[201]
                sgy_header.loc[index,'x[203]'] = x[203]
                sgy_header.loc[index,'x[205]'] = x[205]
                sgy_header.loc[index,'x[211]'] = x[211]
                sgy_header.loc[index,'x[213]'] = x[213]
                sgy_header.loc[index,'x[215]'] = x[215]
                sgy_header.loc[index,'x[217]'] = x[217]
                sgy_header.loc[index,'x[219]'] = x[219]
                sgy_header.loc[index,'x[225]'] = x[225]
                sgy_header.loc[index,'x[231]'] = x[231]
        sgy_header.index = sgy_header.index+1
        return sgy_header

def writeSPS2sgy(segyfile, x_file_path, r_file_path, s_file_path,spsformat):
    x_file, r_file, s_file = sps2df(spsformat, x_file_path, r_file_path, s_file_path) # read original sps file 
    全部有效炮 = np.unique(x_file.loc[:, '野外记录号'])
    with segyio.open(segyfile,'r+',ignore_geometry=True) as seis_data:
        for index,x in tqdm(enumerate(seis_data.header[:])):
            if x[13]==0 or x[13]<0:
                continue
            if x[9] not in 全部有效炮:
                continue
            try:
                检波点,炮点 = get_one_shot_receiver_inSingleRfileLine(x[9],x[13],x_file,r_file,s_file)
                x[1] = index
                x[21] = x[5]#CMP道集号 
                x[41] = 检波点['接收点高程']#检波点高程
                x[45] = 炮点['炮点高程']#震源点高程
                x[49] = 炮点['炮点深度']#井深（从地面起算的正数）
                x[53] = 检波点['基准面']#检波点处的基准面高程
                x[57] = 炮点['基准面']#炮点处的基准面高程
                x[73] = 炮点['炮点X坐标']#震源点X坐标
                x[77] = 炮点['炮点Y坐标']#震源点X坐标
                x[81] = 检波点['接收点X坐标']#检波点X坐标
                x[85] = 检波点['接收点Y坐标']#检波点Y坐标
                x[99] = 炮点['静校正量']#炮点静校正量
                x[101] = 检波点['静校正量'] #检波点静校正量
                x[189] = 检波点['接收点线号']#三维线号
                x[193] = 检波点['接收点点号']#三维点号
                
            except:
                print("error:{}{}",str(x[9]), str(x[13]))
                continue
        print('writeSPS2sgy:  {}    Done!'.format(segyfile))
        
def EditDataFrame_By_SPS(df,x_file_path,r_file_path,s_file_path,spsformat):
    x_file, r_file, s_file = sps2df(spsformat, x_file_path, r_file_path, s_file_path) # read original sps file 
    for i in tqdm(range(len(df))):
        检波点,炮点 = get_one_shot_receiver_inSingleRfileLine(df.loc[i,'x[9]'],df.loc[i,'x[13]'],x_file, r_file, s_file)
        df.loc[i,'x[41]'] = float(检波点['接收点高程'])#检波点高程
        df.loc[i,'x[45]'] = float(炮点['炮点高程'])#震源点高程
        df.loc[i,'x[49]'] = float(炮点['炮点深度'])#井深（从地面起算的正数）
        df.loc[i,'x[53]'] = float(检波点['基准面'])#检波点处的基准面高程
        df.loc[i,'x[57]'] = float(炮点['基准面'])#炮点处的基准面高程
        df.loc[i,'x[73]'] = float(炮点['炮点X坐标'])#震源点X坐标
        df.loc[i,'x[77]'] = float(炮点['炮点Y坐标'])#震源点X坐标
        df.loc[i,'x[81]'] = float(检波点['接收点X坐标'])#检波点X坐标
        df.loc[i,'x[85]'] = float(检波点['接收点Y坐标'])#检波点Y坐标
        df.loc[i,'x[99]'] = float(炮点['静校正量'])#炮点静校正量
        df.loc[i,'x[181]'] = float(炮点['炮点线号']) #炮点线号
        df.loc[i,'x[185]'] = float(炮点['炮点点号']) #炮点点号
        df.loc[i,'x[189]'] = float(检波点['接收点线号'])#接收点线号
        df.loc[i,'x[193]'] = float(检波点['接收点点号'])#接收点点号
    return df


