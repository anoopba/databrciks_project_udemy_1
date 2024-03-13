# Databricks notebook source
# we get the all the path and names in form of lists.
print('OUTPUT1\n')
print(dbutils.fs.ls('/'))
print('\n')

#lets loop the above data and fetch the name
print('OUTPUT2\n')
for file_data in dbutils.fs.ls('/'):
    print(file_data)
print('\n')

#Lets to access an FileInfo objects (FileInfo is a class):
print('OUTPUT3\n')
for file_data in dbutils.fs.ls('/'):
    print(file_data.name)
    print(file_data[1])
    if file_data.name.endswith('/'):
        print(file_data)
    print('\n')


# COMMAND ----------


