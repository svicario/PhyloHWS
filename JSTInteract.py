import urllib2
import urllib
import davlib
import os
import xml.etree.ElementTree as ET
import random


def Upload(File, session):    
    #setup connection and make directory
    url='testjst.ba.infn.it'
    dav = davlib.DAV(url, protocol="http") 
    resp = dav.mkcol("/openacces/"+session)
    if not (200 <= resp.status < 302):
        raise Exception(resp.read())
    dav.close()
    #grab file
    try:
        A=File.read()
    except AttributeError:
        hfile=open(File,"r")
        filepath=File
        A=hfile.read()
        hfile.close()
    else:
        filepath=File.name
        File.close()
    #reopen connection to upload file content
    dav = davlib.DAV(url, protocol="http")
    filename=os.path.basename(filepath)
    resp = dav.put("/openacces/"+session+"/"+filename, A)
    url=resp.getheader("location")
    if not (200 <= resp.status < 300):
        raise Exception(resp.read())
    dav.close()
    return url

def UploadAndSubmitJST(arguments,name,session,mail):
     """
     highlevel function
     """
     FILES=[]
     for key,value in arguments.items():
         if "read" in dir(value):
                 arguments[key]=Upload(value,session)
     jobid=submitJST(arguments,name,session,mail)
     return jobid 

def submitJST(arguments={},name="",session="",mail=""):
    """
    submit a job of JST WS
    """
    #compose the call
    argumentsString=" ".join([x+"::"+arguments[x] for x in arguments])
    base="http://alicegrid17.ba.infn.it:8080/INFN.Grid.RestFrontEnd/services/QueryJob/InsertJobs?"
    NAME="NAME={"+name+"}"
    ARG="arguments={"+argumentsString+"}"
    MAIL="mail="+mail
    Session="session="+session
    URL="&".join([base,NAME,ARG,MAIL,Session])
    A= urllib.urlopen(URL)
    response=A.read()
    #Parse the result
    Element = ET.fromstring(response)
    Element=Element.find(".//JobId")
    jobid=Element.text
    return jobid

def retrive(IdJob):
    """
    Retrieve a job from JST using job identifier
    """
    #compose the call
    base=" http://alicegrid17.ba.infn.it:8080/INFN.Grid.RestFrontEnd/services/QueryJob/SelectJob?"
    jobid="IdJob={"+IdJob+"}"
    #Perform the call
    A= urllib2.urlopen(base+jobid)
    response=A.read()
    #Parse the result
    ElementR = ET.fromstring(response)
    Element=ElementR.find(".//StandardOutput")
    stdoutput=Element.text
    Element=ElementR.find(".//Output")
    output=Element.text
    return output,stdoutput

if __name__ == '__main__':
    treepath="tree.txt"
    samplepath="sample.txt"
    grouppath="group.txt"
    arguments={
    "Nrandomization":"10",
    "qparamHill":"1",
    "output_prefix":"ciccio",
    "XMLoutputType":"phyloxml"}
    name="PhyloH"
    mail="ciccio.bello@gmail.com"
    session=str(int(random.random()*1000000))
    
    # Upload files and add url on dictionary
    treeurl=Upload(treepath,session)
    arguments["tree"]=treeurl
    sampleurl=Upload(samplepath,session)
    arguments["sample"]=sampleurl
    groupurl=Upload(grouppath,session)
    arguments["group"]=groupurl
    
    # Submit job and retrive job identifier   
    JobID=submitJST(arguments,name,session=session,mail=mail)
    Link, Viz=retrive(JobID)
    print(Link)


