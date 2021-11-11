import pyodbc
import pandas as pd
import os
from datetime import datetime
from tkinter import *
from tkinter import filedialog
 
 
#Connect to SQL Server -------------------------------------------------------
#server = ‘xxxxxxx'                          #server name
server = 'xxxxxxxxxx'                     #server name
user = 'xxxxxxxxxx'                                  #username
pword = 'xxxxxxxxx'                                    #password
#-----------------------------------------------------------------------------
prog = 'xxxxxxxx' #program data base to access
SN = 'xxxxxxx' #serial number
 
 
#Browse for files GUI --------------------------------------------------------
files = []
def browseFiles():
    filenames = filedialog.askopenfilenames(initialdir = "/", title = "Select Files",
                                          filetypes = (("csv files", "*.csv*"),("prn files", "*.prn*"),("all files", "*.*")),
                                          multiple = True)
    for file in filenames:
        files.append(file)
 
window = Tk()         # Create the root window
window.title('File Explorer') # Set window title
 
# Create a File Explorer label
label_file_explorer = Label(window, text = "Chose files to load to database", width = 100, height = 4, fg = "blue")
button_explore = Button(window, text = "Browse Files", command = browseFiles)
button_exit = Button(window, text = "Submit", command = window.destroy)
 
label_file_explorer.grid(column = 1, row = 1)
button_explore.grid(column = 1, row = 2)
button_exit.grid(column = 1,row = 3)
 
# Let the window wait for any events
window.mainloop()
#-----------------------------------------------------------------------------
 
 
csv_files = [x for x in files if '.csv' in x]
prn_files = [x for x in files if '.prn' in x]
 
 
#connect to database
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+prog+';UID='+user+';PWD='+ pword)
cursor = cnxn.cursor()
cursor.execute("USE " + prog) #ensures connection to the database of interest
cnxn.setdecoding(pyodbc.SQL_CHAR, encoding='latin1')
cnxn.setencoding('latin1')
 
 
#retrieve dutInfo_ID
cursor.execute("SELECT dutInfo_ID FROM tbl_dutInfo WHERE dutSubA_SN = '" +SN+ "'")
for i in cursor:
    dutInfo_ID = str(i)[1:-3]
 
 
#-----------------------------------------------------------------------------
df = pd.DataFrame(columns = ['vector_ID', 'dutInfo_ID', 'test_Port', 'test_Phase', 'test_Parameter', 'test_Desc', 'meansDateTime', 'temp',
                             'sav_Dir', 'fileName', 'equipment_SN', 'station_ID', 'technician', 'archive', 'approved', 'data_warehouse'])
 
counter = 0
if len(csv_files) > 0:
    for file in csv_files: 
        file_split = file.split('/') 
        filename = file_split[5]
       
        namesplit = filename.split(' ')
           
        df.loc[counter, 'dutInfo_ID'] = dutInfo_ID
        df.loc[counter, 'test_Phase'] = namesplit[1]
        df.loc[counter, 'test_Parameter'] = namesplit[3]
        df.loc[counter, 'test_Desc'] = namesplit[2]       
        df.loc[counter, 'meansDateTime'] = datetime.utcfromtimestamp(os.path.getmtime(csv_files[counter])).isoformat(sep=' ', timespec='milliseconds')
        df.loc[counter, 'sav_Dir'] = csv_files[counter]       
        df.loc[counter, 'fileName'] = filename
        df.loc[counter, 'equipment_SN'] = 'UPLOAD'
        df.loc[counter, 'archive'] = 1
       
        counter += 1
       
        
if len(prn_files)>0:
    for file in prn_files:       
        file_split = file.split('/')   
        filename = file_split[5]
       
        namesplit = filename.split(' ')
       
        df.loc[counter, 'dutInfo_ID'] = dutInfo_ID
        df.loc[counter, 'test_Phase'] = namesplit[1]
        df.loc[counter, 'test_Parameter'] = namesplit[3]
        df.loc[counter, 'test_Desc'] = namesplit[2]      
        df.loc[counter, 'meansDateTime'] = datetime.utcfromtimestamp(os.path.getmtime(prn_files[counter - len(csv_files)])).isoformat(sep=' ', timespec='milliseconds')
        df.loc[counter, 'sav_Dir'] = prn_files[counter - len(csv_files)]      
        df.loc[counter, 'fileName'] = filename
        df.loc[counter, 'equipment_SN'] = 'UPLOAD'
        df.loc[counter, 'archive'] = 1
       
        counter += 1
   
df.meansDateTime = pd.to_datetime(df.meansDateTime)
df.sort_values(by = 'meansDateTime', inplace=True)
df.reset_index(drop=True, inplace=True)
 
#-----------------------------------------------------------------------------   
 
#new entries in tbl_vector
# QUERY BY DUTINFO_ID, PHASE, PARAMETER, DESCRIPTION AND ARCHIVE = 1
# IF THAT EXISTS, SWITCH ITS ARCHIVE TO 0 AND CONTINUE
# IF NOT, CONTNINUE   
for j in range(0, len(df)):
   
    query = "SELECT * FROM tbl_vector WHERE dutInfo_ID="+df.loc[j,'dutInfo_ID']+" AND test_Parameter='"+df.loc[j,'test_Parameter']+"'" +\
        " AND test_Phase='"+df.loc[j,'test_Phase']+"' AND test_Desc='"+df.loc[j,'test_Desc']+"' AND archive=1"
    cursor.execute(query)
 
    isitalreadyhere=[]
   
    for i in cursor:
        isitalreadyhere.append(i)
   
    
    if len(isitalreadyhere) > 0:
        query = "UPDATE tbl_vector SET archive=0 WHERE dutInfo_ID="+df.loc[j,'dutInfo_ID']+" AND test_Parameter='"+df.loc[j,'test_Parameter']+"'" +\
        " AND test_Phase='"+df.loc[j,'test_Phase']+"' AND test_Desc='"+df.loc[j,'test_Desc']+"' AND archive=1"
        cursor.execute(query)



   
    query = "INSERT INTO tbl_vector (dutInfo_ID, test_Phase, test_Parameter, test_Desc, measDateTime,"+\
    " sav_Dir, fileName, equipment_SN, archive) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
   
    query_tuple = [(df.loc[j, 'dutInfo_ID'],df.loc[j, 'test_Phase'], df.loc[j, 'test_Parameter'],
                    df.loc[j, 'test_Desc'], df.loc[j, 'meansDateTime'],df.loc[j, 'sav_Dir'],df.loc[j, 'fileName'], df.loc[j, 'equipment_SN'], 1)]
   
    cursor.executemany(query, query_tuple)
    cursor.commit()
   
    #grab the new vector_IDs
    cursor.execute("SELECT vector_ID FROM tbl_vector WHERE fileName = '" +df.loc[j, 'fileName']+ "'")
    for i in cursor:
        df.loc[j, 'vector_ID'] = str(i)[1:-3]   
    
#-----------------------------------------------------------------------------  
 
# uploads .csv files to tbl_vectorData
for file in csv_files:
    data_df = pd.read_csv(file)
    data_df = data_df.reset_index(drop=False)
    data_df = data_df.reset_index(drop=False)
 
    data_df.drop(data_df.columns[len(data_df.columns)-1], axis=1, inplace=True)
    data_df.drop([0], inplace = True)
    data_df.columns = ['ptNum', 'x', 'y']
 
    df_row = df[df.sav_Dir == file]
    vec_id = df_row.vector_ID.to_list()[0]
   
    data_df.insert(0, 'vector_ID', [vec_id]*len(data_df))
 
    for pt in range(1, len(data_df)):
        query = "INSERT INTO tbl_vectorData (vector_ID, ptNum, x, y) VALUES (?,?,?,?)"
        query_tuple = [(int(data_df.loc[pt, 'vector_ID']), int(data_df.loc[pt, 'ptNum']), float(data_df.loc[pt, 'x']), float(data_df.loc[pt, 'y']))]
       
        cursor.executemany(query, query_tuple)
        cursor.commit()   
#-----------------------------------------------------------------------------
 
#uploads .prn files to tbl_vectorData
for file in prn_files:
    data = open(file, 'r')   
    data = data.readlines()
    del data[0]
    del data[0]
       
    data = pd.Series(data)
    data = data.str.split(pat = ',', expand = True)
    data.reset_index(drop = False, inplace = True)
    data.drop(data.columns[len(data.columns)-1], axis=1, inplace=True)
    data.columns = ['ptNum', 'x', 'y']
    data['ptNum'] = data['ptNum']+1
    df_row = df[df.sav_Dir == file]
    vec_id = df_row.vector_ID.to_list()[0]
    data.insert(0, 'vector_ID', [vec_id]*len(data))
   
    for pt in range(0, len(data)):
        query = "INSERT INTO tbl_vectorData (vector_ID, ptNum, x, y) VALUES (?,?,?,?)"
        query_tuple = [(int(data.loc[pt, 'vector_ID']), int(data.loc[pt, 'ptNum']), float(data.loc[pt, 'x']), float(data.loc[pt, 'y']))]
       
        cursor.executemany(query, query_tuple)
        cursor.commit()
   
 
   
cursor.close()   #close the cursor
cnxn.close()     #close the connection
