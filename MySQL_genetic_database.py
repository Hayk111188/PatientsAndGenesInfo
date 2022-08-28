import mysql.connector
import json 
import csv

# Sql connection config
connection_config_dict = {
  'host':'localhost',
  'user':'root',
  'password':'root123'
}

# Creating genes db from benchling_entries.json
mydb = mysql.connector.connect(**connection_config_dict)
cursor = mydb.cursor()
cursor.execute("CREATE DATABASE IF NOT EXISTS GenesDb",multi=True) 
     
# Loading json data
json_path = "C://Python projects//MySQL genetic database//benchling_entries.json"
f = open(json_path);
data = json.load(f)      
f.close()
    
entries = data["entries"];

#Create 2 tables for up/down regulations and inserting clients data
for entry in entries:
    client_id=entry["fields"]["a. Patient ID"]["textValue"]
    
    for day in entry["days"]:
        for note in day["notes"]:
            if "table" in note:
                name_query=""
                table_Name=note["table"]["name"]
                if (table_Name!="Genes to down regulate" and table_Name!="Genes to up regulate"):
                    continue
                column_Names=note["table"]["columnLabels"]
                for column_name in column_Names:
                    name_query += "`"+column_name + "` VARCHAR(255),"
                query=   "Use GenesDb; CREATE TABLE IF NOT EXISTS `"+table_Name+"` ( "+"ClientId VARCHAR(255),"+ name_query[:-1]+");DELETE * FROM `"+ table_Name+"`"                
                mydb = mysql.connector.connect(**connection_config_dict)
                cursor = mydb.cursor()
                cursor.execute(query,multi=True)
                                                               
                rows= note["table"]["rows"]              
                for row in rows:
                    cell_Values="'"+client_id+"',"
                    cells=row["cells"]
                    for cell in cells:
                        cell_Values+="'"+cell["text"]+"',"
                    value_query="("+cell_Values[:-1] + ")"    
                    query=   "USE GenesDb; INSERT INTO `"+table_Name+"` VALUES" + value_query  +";commit;" 
                    mydb = mysql.connector.connect(**connection_config_dict)
                    cursor=mydb.cursor()
                    cursor.execute(query,multi=True)
                    mydb.close()

# Create table for Client Info from cnv_processed.txt

query=   "Use GenesDb; CREATE TABLE IF NOT EXISTS ClientInfo(ClientId VARCHAR(255), CopyNumber VARCHAR(255), Symbol VARCHAR(255))"                
mydb = mysql.connector.connect(**connection_config_dict)
cursor = mydb.cursor()
cursor.execute(query,multi=True)

# Reading data from text file and inserting data: PatientId/CopyNumber/Symbol
txt_path = 'C://Python projects//MySQL genetic database//cnv_processed.txt'
with open(txt_path, newline = '') as patients:     
     reader = csv.reader(patients, delimiter='\t')
     for patient in reader:
          if patient[17]=="Patient_ID":
              continue
          query=   "USE GenesDb; INSERT INTO ClientInfo VALUES('" + patient[17]+"','"+patient[7]+"','"+patient[12]+"')"  +";commit;" 
          mydb = mysql.connector.connect(**connection_config_dict)
          cursor=mydb.cursor()
          cursor.execute(query,multi=True)



mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="root123",
  database="GenesDb"
)
# Count of patients that have both up and down regulated genes
query1= "SELECT count(ClientId) FROM (SELECT up.ClientId FROM `Genes to up regulate` up INNER JOIN `Genes to down regulate` down on down.ClientId=up.ClientId group by up.ClientId) AS T" 

# Count of patients that have CopyNumber 
query2= "SELECT count(*) FROM (SELECT distinct ClientId FROM (SELECT * FROM ClientInfo where CopyNumber is not null) AS T) AS T" 

# Count of patients that have CopyNumber and up/down regulations
query3= "SELECT distinct(ClientId) FROM (SELECT up.ClientId FROM `Genes to up regulate` up INNER JOIN `Genes to down regulate` down on down.ClientId=up.ClientId where down.ClientId in (select ClientId from ClientInfo)) AS T" 

cursor = mydb.cursor()

cursor.execute(query1, multi=True)
result1 = cursor.fetchone()

cursor.execute(query2, multi=True)
result2 = cursor.fetchone()

cursor.execute(query3, multi=True)
result3 = cursor.fetchall()

print(result1[0])
print(result2[0])

for row in result3:
    print(row[0])

cursor.close()
mydb.close()






                   
cursor.close()
mydb.close() 
print("Finished")
                   
                   



