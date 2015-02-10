import urllib2
import urllib
import davlib
import os
import xml.etree.ElementTree as ET
import random
import json

#davlib is taken from Python_WebDAV_library 0.4.2

urlwebdav='testjst.ba.infn.it'

def getInfo(MethodNumber=None):
    """
    Get Info on method available on JST WS and of specific input needs of each one
    Info are stored on biodiversitycatalogue
    """
    if not MethodNumber:
        A= urllib.urlopen("https://www.biodiversitycatalogue.org/rest_services/23.xml")
        Response=A.read()
        Element = ET.fromstring(Response)
        ElementA=Element.findall("./{http://www.biocatalogue.org/2009/xml/rest}resources/{http://www.biocatalogue.org/2009/xml/rest}restResource")
        Info=[]
        for x in  ElementA:
            url=x.attrib["{http://www.w3.org/1999/xlink}href"]
            A=urllib.urlopen(url+".json").read()
            Info.append(json.loads(A)['rest_resource'])
    elif MethodNumber:
        url="https://www.biodiversitycatalogue.org/rest_methods/"+MethodNumber+"/inputs"
        Inputspec=json.loads(urllib.urlopen(url+".json").read())
        Info=Inputspec[u'rest_method']
    return Info


def MakeDirectory(foldername):
    dav = davlib.DAV(urlwebdav, protocol="http") 
    resp = dav.mkcol("/openacces/"+foldername)
    if not (200 <= resp.status < 302):
        raise Exception(resp.read())
    dav.close()


def Upload(File, path=None, makepath=False):
    if makepath:
        #setup connection and make directory
        MakeDirectory(path)
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
    dav = davlib.DAV(urlwebdav, protocol="http")
    filename=os.path.basename(filepath)
    resp = dav.put("/openacces/"+path+"/"+filename, A)
    url=resp.getheader("location")
    if not (200 <= resp.status < 300):
        raise Exception(resp.read())
    dav.close()
    return url


def submitJST(arguments={},name="",session="",mail=""):
    """
    submit a job to JST WS
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


def UploadAndSubmitJST(arguments,name,session,mail):
     """
     highlevel function
     """
     FILES=[]
     first=True
     for key,value in arguments.items():
         if "read" in dir(value):
            if first:
                MakeDirectory(session)
                first=False
            arguments[key]=Upload(value,session)
     jobid=submitJST(arguments,name,session,mail)
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


