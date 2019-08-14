'''
Created on Nov 27, 2015

@author: vtrivedi
'''
from pyspark import SparkConf, SparkContext
#from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix
from thunder import ThunderContext
import sys
import shutil, os
from datetime import datetime    

def ReadConfFile(folderPath):
    import json
    with open(folderPath + "conf.json") as data_file:    
        confObj = json.load(data_file)
    return confObj
    

def roswForMatRDD(imgRdds):
    iImg=0
    idxRowMat = []
    for rddElement in imgRdds.collect():
        mat=rddElement
        iRow=0
        for x in mat:
            idxRowMat.append((iImg,(iRow, x.tolist())))
            iRow=iRow+1
        iImg=iImg+1
    print idxRowMat    
    return idxRowMat

def createCombiner(a):
    return [a]

def mergeValue(a, b):
    return a+b

def removeRCIndex(l):
    imgMatrix=[]
    for row in l:
        for cols in row[1]:
            imgMatrix.append(cols[1])
    return imgMatrix            
    
    

def CompressImages(inputs, output, confObj):
    debugMode=False
    st=datetime.now()
    imageExt = confObj['ext']
    imageHeight = confObj['dims'][0]
    imageWidth = confObj['dims'][1]
    refImgId = confObj['refImageId']
    diffImageFolder = confObj['DiffImageFolder']

    if debugMode == True:
        print confObj
    
    import glob
    totImages = len(glob.glob(inputs+"*."+imageExt))

    if os.path.exists(output):
        shutil.rmtree(output)
    
    conf = SparkConf().setAppName('ImgCompress')
    sc = SparkContext(conf=conf)
    imageHeight = sc.broadcast(imageHeight)
    imageWidth = sc.broadcast(imageWidth)
    
    tsc = ThunderContext(sc)
    tscImages = tsc.loadImages(inputs, (imageHeight.value,imageWidth.value), imageExt, imageExt).cache()
    
    floatingPixelRdd = tscImages.rdd.flatMapValues(lambda r: r).zipWithIndex().map(lambda l: ((l[0][0],(l[1]-(l[0][0]*int(imageHeight.value)))),l[0][1]))\
        .flatMapValues(lambda r: r).zipWithIndex().\
        map(lambda l: ((l[0][0][1],(l[1]-(l[0][0][0]*int(imageWidth.value)*int(imageHeight.value) + l[0][0][1]*int(imageWidth.value)))),(l[0][0][0],l[0][1])))
    
    if debugMode == True:
        floatingPixelRdd.saveAsTextFile(output+"\\Stage-1-FloatingPixel")    
    
    temporalVoxelRdd = floatingPixelRdd.groupByKey().map(lambda x : (x[0], list(x[1]))).cache()
    if debugMode == True:
        temporalVoxelRdd.saveAsTextFile(output+"\\Stage-2-TemporalVoxel")    
    
    iMapImageDict = {}
    for imgIndex in range(totImages):
        imgIndexBD = sc.broadcast(imgIndex)
        
        #-------------------------------HELPER FUNCTIONS-------------------------------------
        def ReAdjustImapList(l):
            intList = l[1]
            mKey = intList.pop(imgIndexBD.value)[1]
            return (mKey, intList)

        def calculateMedainForEachImage(l):
            intRdd = sc.parallelize(l[1]).flatMap(lambda l: l).groupByKey().map(lambda x : (x[0], list(x[1])))
            intRddSorted = intRdd.map(lambda l: (l[0],sorted(l[1])))
            intRddMedian = intRddSorted.map(lambda l: (l[0], l[1][int(len(l[1])/2)])) 
            return intRddMedian.collect()
        #-------------------------------HELPER FUNCTIONS-------------------------------------
        
        
        imapRdd = temporalVoxelRdd.map(lambda l: ReAdjustImapList(l)).sortBy(lambda  l: l[0], False)\
            .groupByKey().map(lambda x : (x[0], list(x[1])))
        if debugMode == True:
            imapRdd.saveAsTextFile(output+"\\Stage-3__IMAP-Stage-1_imgIdx-" + str(imgIndex))    

        
        imapRdd = imapRdd.flatMapValues(lambda l: l).flatMapValues(lambda l: l).map(lambda l: ((l[0],l[1][0]),l[1][1])).\
            groupByKey().map(lambda x : (x[0], list(x[1]))).\
            map(lambda l: (l[0],sorted(l[1]))).map(lambda l: (l[0], l[1][int(len(l[1])/2)])).\
            map(lambda l: (l[0][0],(l[0][1], l[1])))
        if debugMode == True:
            imapRdd.saveAsTextFile(output+"\\Stage-3__IMAP-Stage-2_imgIdx-" + str(imgIndex))    
        
        imapRdd = imapRdd.groupByKey().map(lambda x : (x[0], sorted(list(x[1]),key = lambda k: k[0])))
        if debugMode == True:
            imapRdd.saveAsTextFile(output+"\\Stage-3__IMAP-Stage-3_imgIdx-" + str(imgIndex))    

        imapDict = imapRdd.collectAsMap()
        iMapImageDict[imgIndex] = imapDict  

        
    iMapDictBD = sc.broadcast(iMapImageDict)
    
    refImageIdBD=sc.broadcast(refImgId)
    
    def CalcMapValue(pix, iMapVal):
        pImgIdx = pix[0]
        residual=0
        for iIdx in iMapVal:
            if pImgIdx == iIdx[0]:
                residual = int(iIdx[1])  - pix[1]
                break
        return (pix[0], residual)
        
        
    def ApplyIMap(l):
        voxel = l[1]
        refIntensity = l[1][refImageIdBD.value][1]
        iMapValues= iMapDictBD.value[refImageIdBD.value][refIntensity]
        resdualValues=[]
        for pixel in voxel:
            if pixel[0] != refImageIdBD.value:
                resdualValues.append( CalcMapValue(pixel, iMapValues) )
            else:
                resdualValues.append( pixel )
                
        return (l[0], resdualValues)         
    
    
    diffFVRdd = temporalVoxelRdd.map(lambda l: ApplyIMap(l))
    if debugMode == True:
        diffFVRdd.saveAsTextFile(output+"\\Stage-4__Diff-Stage-1")    

    residualImages = diffFVRdd.flatMapValues(lambda l: l).map(lambda l: ((l[1][0], l[0][0]),(l[0][1],l[1][1])))\
        .groupByKey().map(lambda x : (x[0], sorted(list(x[1]),key = lambda k: (k[0]))))\
        .map(lambda l: (l[0][0],(l[0][1],l[1])))\
        .groupByKey().map(lambda x : (x[0], sorted(list(x[1]),key = lambda k: (k[0]))))\
        .map(lambda l: (l[0], removeRCIndex(l[1])))

    residualImages.saveAsTextFile(output+"\\"+diffImageFolder)          

    for ingIdx in range(totImages):
        iMapImageDict[str(ingIdx)] = dict([(str(k), str(v)) for k, v in iMapImageDict[ingIdx].items()])
        del iMapImageDict[ingIdx]
    
    import json
    with open(output + "\\imap.json",'w') as f:
        json.dump(iMapImageDict,f)    

    with open(output + '\\conf.json', 'w') as f:
        json.dump(confObj, f)
    
    # clean up
    sc.stop()
    
    en=datetime.now()
    td = en-st
    print td

def ReadIntensityMap(folderPath):
    import json
    with open(folderPath + "\\imap.json") as data_file:    
        iMapDict = json.load(data_file)
        
    from ast import literal_eval
    iMapDict = dict([(int(k1), dict([(int(k), literal_eval(v)) for k, v in v1.items()])) for k1, v1 in iMapDict.items() ])
    return iMapDict


def readDataFiles(l):
    from ast import literal_eval
    f = open(l, 'r')
    imgMatObj= literal_eval(f.read())
    f.close()
    return imgMatObj
        
def deCompressImages(targetFolder, confObj, iMapDict):
    conf = SparkConf().setAppName('ImgDeCompress')
    sc = SparkContext(conf=conf)
    

    imageType = confObj['ext']
    imageHeight = confObj['dims'][0]
    imageWidth = confObj['dims'][1]
    refImageId = confObj['refImageId']
    diffImageFolder = confObj['DiffImageFolder']
    constructedImageFolder = confObj['ConstructedImageFolder']

    if os.path.exists(targetFolder + constructedImageFolder):
        shutil.rmtree(targetFolder + constructedImageFolder)

    
    import glob
    fileList = glob.glob(targetFolder + diffImageFolder +"\\p*")
    totImages = len(fileList)

    iMapDictBD = sc.broadcast(iMapDict)
    refImageIdBD=sc.broadcast(refImageId)
    imageHeight = sc.broadcast(imageHeight)
    imageWidth = sc.broadcast(imageWidth)
    imageTypeBD = sc.broadcast(imageType)
    constructedImageFolderBD = sc.broadcast(constructedImageFolder)
    targetFolderBD = sc.broadcast(targetFolder)
 
    diffImages=sc.parallelize(fileList, totImages).map(lambda v:  readDataFiles(v)).cache()

    tVoxel = diffImages.flatMapValues(lambda r: r).zipWithIndex()\
        .map(lambda l: (l[1]-(l[0][0]*int(imageHeight.value)*int(imageWidth.value)),(l[0][0],l[0][1])))\
        .groupByKey().map(lambda x : (x[0], list(x[1]))).cache()

    
    def addMapValue(pix, iMapVal):
        pImgIdx = pix[0]
        whole=0
        for iIdx in iMapVal:
            if pImgIdx == iIdx[0]:
                if pix[1] < 0:
                    whole = int(iIdx[1]) + abs(pix[1])
                else:
                    whole = int(iIdx[1]) - pix[1]
                break
        return (pix[0], whole)
        
        
    def ApplyIMap(l):
        voxel = l[1]
        refIntensity = l[1][refImageIdBD.value][1]
        iMapValues= iMapDictBD.value[refImageIdBD.value][refIntensity]
        resdualValues=[]
        for pixel in voxel:
            if pixel[0] != refImageIdBD.value:
                resdualValues.append( addMapValue(pixel, iMapValues) )
            else:
                resdualValues.append( pixel )
        return (l[0], resdualValues)         
    
    def extractIntensity(l):
        intValues=[]
        for itm in l:
            intValues.append(itm[1][1])
        return intValues
    
    def saveImages(l):
        from PIL import Image
        import numpy as np
        aa=np.array(l[1], np.uint8).reshape(imageWidth.value, imageHeight.value)
        pilImg = Image.fromarray(aa)
        imgStr = str(l[0])+"."+ imageTypeBD.value
        print targetFolderBD.value + constructedImageFolderBD.value + "\\img_"+ imgStr
        pilImg.save(targetFolderBD.value + constructedImageFolderBD.value + "\\img_"+ imgStr)
    
    if not os.path.exists(targetFolder + constructedImageFolder):
        os.makedirs(targetFolder + constructedImageFolder)
    
    originalVoxels = tVoxel.map(lambda l: ApplyIMap(l))
    imgPixelsRdd = originalVoxels.flatMapValues(lambda r: r).map(lambda r: (r[1][0],(r[0],r[1])))\
        .groupByKey().map(lambda x : (x[0], sorted(list(x[1]),key = lambda k: k[0])))\
        .map(lambda l: (l[0],extractIntensity(l[1]))).map(lambda l: saveImages(l))
    imgPixelsRdd.collect()

    sc.stop()
 
def main():
    op = sys.argv[1]
    if op=='c':
        imageFolder = sys.argv[2] + "\\"
        outputFolderPath = sys.argv[3]
        confObj= ReadConfFile(imageFolder)
        CompressImages(imageFolder, outputFolderPath, confObj)    
    elif op == 'd':
        outputFolderPath = sys.argv[2] + "\\"
        confObj= ReadConfFile(outputFolderPath)
        iMapDict=ReadIntensityMap(outputFolderPath)
        deCompressImages(outputFolderPath, confObj, iMapDict)
    
if __name__ == "__main__":
    main()

