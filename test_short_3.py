#!/usr/bin/env python
# coding: utf-8


# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import base64
from io import BytesIO
from io import TextIOWrapper
from io import StringIO
import streamlit as st
from streamlit import caching
from pandas.api.types import is_string_dtype
import copy
import hashlib




def get_naming_params():
    """ 
    creates dictionary with parameters for running session
    """     
    namingParams={
            "datasetListDict":"datasetListDict",
            "fileFormat":"fileFormat",
            "dataUploaded":"dataUploaded",
            "datasetName":"datasetName",
            "fileSeparator":"fileSeparator",
            "isdataset":"isdataset",
            "chooseColumnLabel":"🔽 Filter column",
            "nothingFilteredName":"None",
            "filterDictName":"filterDictName",
            "fileUploadChoice":"I will upload a file",
            "fileChoiceLabel":"📁 Choose a sample dataset...",
            "fileCodeName":"fileCode",
            "fileUploadLabel":"📤 ...or upload your csv or Excel file",
            "encoding":"encoding",  
            "columnHash":"columnHash",
            "notMetConditionValue":False,                                             
               }
    return namingParams

def get_file_params():
    """ 
    creates dictionary with file saving parameters
    """  
    fileParams={   
            "sepCsv":";",
            "encodingUTF8":"utf-8",
            "encodingISO":"ISO-8859-1",
                }   
    return fileParams


def main():
    paramDict={}
    df,paramDict=load_dataset(paramDict)
    df,indexCols,valueCols,paramDict=build_initial_index_array(df,paramDict)
    paramDict=get_column_hash(indexCols,paramDict)
    paramDict=make_filter_dict(df,indexCols,paramDict)      
    return None


def hashFor(data):
    # Prepare the project id hash
    hashId = hashlib.md5()
    hashId.update(repr(data).encode('utf-8'))
    return hashId.hexdigest()

def get_column_hash(array,paramDict):
    namingParams=get_naming_params()
    notMetConditionValue=namingParams["notMetConditionValue"] 
    columnHash=namingParams["columnHash"] 
    if len(array)>0:
        paramDict[columnHash]=hashFor(array)
    else:    
        paramDict[columnHash]=notMetConditionValue
    return paramDict  

def get_hashed_key(key,columnHash):
    if columnHash:
        key=key+str(columnHash)
    return key


def build_initial_index_array(df,paramDict):
    namingParams=get_naming_params()
    columns=list(df.columns.values) 
    indexCols=[]
    valueCols=[]
    toDrop=[]
    for column in columns:  
        if is_string_dtype(df[column]) :
                indexCols.append(column)
    newIndexCols=[]
    for indexCol in indexCols:
        if indexCol not in toDrop:
            newIndexCols.append(indexCol)      
    columns=list(df.columns.values) 
    for column in newIndexCols:
        if column in columns:
            df[column]=df[column].astype(str)        
    return df,newIndexCols,valueCols,paramDict

def show_run_page(paramDict):
    namingParams=get_naming_params()
    dataUploaded=namingParams["dataUploaded"]
    isdataset=namingParams["isdataset"]
    chosenFileUpload,paramDict=set_up_load_data_widgets(paramDict)
    if paramDict[isdataset]:
      if chosenFileUpload != None:
            paramDict[dataUploaded] = chosenFileUpload
            paramDict[isdataset]=True
    return paramDict


def set_up_load_data_widgets(paramDict):
    """
    setting up widgets for data load
    """ 
    namingParams=get_naming_params()  
    isdataset=namingParams["isdataset"]
    paramDict[isdataset]=False
    chosenFileUpload,paramDict=get_uploaded_file(paramDict)
    if chosenFileUpload != None:
        paramDict[isdataset]=True
    return chosenFileUpload,paramDict

def get_uploaded_file(paramDict):
    """
    sets up the widget to upload a file
    """
    namingParams=get_naming_params()
    fileParams=get_file_params()  
    fileUploadLabel=namingParams["fileUploadLabel"]
    encodingISO=fileParams["encodingISO"]
    encodingUTF8=fileParams["encodingUTF8"]  
    result=None 
    uploadedFile = st.file_uploader(fileUploadLabel, type=['csv'],accept_multiple_files=False,key="fileUploader")      
    if uploadedFile is not None:
        try:
            try:
                bytesData = uploadedFile.getvalue()
                encoding = encodingUTF8 
                s=str(bytesData,encoding)
                result = StringIO(s) 
            except:
                bytesData = uploadedFile.getvalue()
                encoding = encodingISO 
                s=str(bytesData,encoding)
                result = StringIO(s)  
            paramDict[namingParams["encoding"]]=encoding                       
        except:
            result=None               
    return result,paramDict

def get_files_from_upload_or_disk(paramDict):
    namingParams=get_naming_params()
    dataUploaded=namingParams["dataUploaded"]
    fileParams=get_file_params()
    if dataUploaded in paramDict and paramDict[dataUploaded] is not None: 
        df=parse_uploaded_file(paramDict)
    else:
        df=pd.DataFrame()
    if dataUploaded in paramDict:
      paramDict[dataUploaded]=True  
    return df,paramDict

def load_dataset(paramDict):
    paramDict=show_run_page(paramDict)
    df,paramDict=get_files_from_upload_or_disk(paramDict)
    return df,paramDict

@st.cache(suppress_st_warning=True,allow_output_mutation=True,show_spinner=False,ttl=1800)
def parse_uploaded_file(paramDict):
    """
    we want to parse the file into a dataframe
    """  
    fileParams=get_file_params()
    namingParams=get_naming_params()
    encoding=namingParams["encoding"]
    dataUploaded=namingParams["dataUploaded"]
    dataUploaded=paramDict[dataUploaded]
    df=pd.read_csv(dataUploaded, error_bad_lines=False, warn_bad_lines=False,sep=",")
    return df

def set_page_config():
    """
    setting up the streamlit config options
    """
    st.set_page_config(
    layout="wide",
    initial_sidebar_state="collapsed",
     )
    return None

def make_filter_dict(df,indexCols,paramDict):
    namingParams=get_naming_params()
    nothingFilteredName=namingParams["nothingFilteredName"]
    filterDictName=namingParams["filterDictName"]
    filterDict={}
    if len(indexCols)>0 :
          indexColsSelectBox=copy.deepcopy(indexCols)
          indexColsSelectBox.insert(0, nothingFilteredName)
          number=1
          filterDict,toFilter,indexColsSelectBox=get_items_to_filter(indexColsSelectBox,paramDict,filterDict,number) 
          if toFilter and number==1:
            number=2
            filterDict,toFilter,indexColsSelectBox=get_items_to_filter(indexColsSelectBox,paramDict,filterDict,number)
          if toFilter and number==2:
            number=3
            filterDict,toFilter,indexColsSelectBox=get_items_to_filter(indexColsSelectBox,paramDict,filterDict,number) 
          if toFilter and number==3:
            number=4
            filterDict,toFilter,indexColsSelectBox=get_items_to_filter(indexColsSelectBox,paramDict,filterDict,number)     
    paramDict[filterDictName]=filterDict  
    return paramDict  

def take_filtered_value_out_of_option_list(inputArray,filterColumn):
    """
    to take out the already filtered options from the option list not possible to use remove, need to create new array
    """
    newArray=[]
    selectionArray=copy.deepcopy(inputArray)
    for element in selectionArray:
        if element != filterColumn:
            newArray.append(element)
    return newArray 


def get_items_to_filter(indexColsSelectBox,paramDict,filterDict,number):
    """
    opens selectbox widgets and returns dictionary of items to include and exclude for given column
    """
    namingParams=get_naming_params()
    chooseColumnLabel=namingParams["chooseColumnLabel"]
    nothingFilteredName=namingParams["nothingFilteredName"] 
    columnHash=namingParams["columnHash"] 
    chooseColumnLabel=chooseColumnLabel+" #"+str(number)
    columnHash=paramDict[columnHash]    
    key=get_hashed_key("filterColumn"+str(number),columnHash)  
    tooltip="""Select a column you want to filter on."""   
    filterColumn=st.selectbox(chooseColumnLabel,indexColsSelectBox,help=tooltip,index=0,
                                    key=key)
    st.write("value returned by widget ==> ",filterColumn)
    st.write("array for selection ==> ",indexColsSelectBox)
    if filterColumn not in indexColsSelectBox:
        filterColumn = nothingFilteredName   
    toFilter=False 
    if filterColumn != nothingFilteredName:
        indexColsSelectBox=take_filtered_value_out_of_option_list(indexColsSelectBox,filterColumn)
        filterDict[filterColumn]={}
    else:
        toIncludeItemsArray,toExcludeItemsArray=[],[]
    if not toFilter:
      filterDict.pop(filterColumn, None)
    return filterDict,toFilter,indexColsSelectBox

main()

   