# -*- coding: utf-8 -*-
"""
Created on Wed Nov  6 13:47:44 2019

@author: Ayappa
"""

from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row
import re
import json

from pprint import pprint

if __name__ == '__main__':
    
    def parseInput(line): 
        
        if (line == '[' ) or (line == ']')  or  (line == ''):
            return "#"
        else:
            text=line.split('{')
            text=text[1].split('}')
            
            titleText=text[0].split('"titleText"')
            titleText=titleText[1].split('[')
            titleText=titleText[1].split(']')
            if not titleText[0]:
                titleText[1]="NA"
            else:
                titleText=titleText[0].split('"')
            
            priceWhole=text[0].split('"priceFraction"')
            priceWhole=priceWhole[1].split('[')
            priceWhole=priceWhole[1].split(']')
            if not priceWhole[0]:
                priceWhole[1]="NA"
            else:
                priceWhole=priceWhole[0].split('"')
                
            partPrice=text[0].split('"priceWhole"')
            partPrice=partPrice[1].split('[')
            partPrice=partPrice[1].split(']')
            if not partPrice[0]:
                partPrice[1]="NA"
            else:
                partPrice=partPrice[0].split('"')
                
            
            rating=text[0].split('"rating"')
            rating=rating[1].split('[')
            rating=rating[1].split(']')
            if not rating[0]:
                rating[0]="0"
            else:
                rating=rating[0].split('"')
                rating=rating[1].split(' out')
                
            
            totalPeopleRated=text[0].split('"totalPeopleRated"')
            totalPeopleRated=totalPeopleRated[1].split('[')
            totalPeopleRated=totalPeopleRated[1].split(']')
            if not totalPeopleRated[0] :
                totalPeopleRated[0]="0"
           
            else:
                totalPeopleRated=totalPeopleRated[0].split('"')
                if re.match(("[a-zA-Z]+"), totalPeopleRated[1], flags=0) or re.match(("[\\\]+"), totalPeopleRated[1], flags=0) :                                    
                    totalPeopleRated[0]="0"
                else:                   
                    totalPeopleRated=totalPeopleRated[1].split(',')              
               
                    
               
            
            imageUrl=text[0].split('"imageUrl"')
            imageUrl=imageUrl[1].split('[')
            imageUrl=imageUrl[1].split(']')
            if not imageUrl[0]:
                imageUrl[1]="NA"
            else:
                imageUrl=imageUrl[0].split('"')
                
            website=text[0].split('"website"')
            
            if not website[1]:
                website[1]="NA"
            else:
                website=website[1].split('"')
            
            #return  int(totalPeopleRated[0])
            return(titleText[1] , (priceWhole[1],partPrice[1], float(rating[0]), int(totalPeopleRated[0]) ,imageUrl[1],website[1]))
       
    
    def comput(title,index):
        titles=title.split(" ")
        for title in titles:
					
            return title,index
        
    def computBi(title,index):
        titles=title.split(" ")
        for x in range(len(titles)):
            if(x+1< len(titles)):        
                string=str(titles[x]+" "+titles[x+1])	  
                return string,index
            else:
                return titles[x],index
        
    def parseclean(line):
        if (line == ']' ):
            return ',{},'
        elif (line == '[' ) :
            return "{},"
        else:
            return line
        
    def calculate(line):                 
        if (line == '#' ) :             
            return ("#",0)
        if(line[1][1]=='Na') or (line[1][2]=='Na'):
            return (line[0],0)
        return (line[0],(line[1][2]*line[1][3]),(line[1][0],line[1][1],str(line[1][2]),str(line[1][3]),line[1][4],line[1][5]))
        

    conf=SparkConf().setAppName("Index").set("spark.driver.allowMultipleContexts", "true")
    sc=SparkContext(conf=conf)

    spark = SparkSession.builder \
        .master("local") \
        .appName("Word Count") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    fileName=["wallmart.json","amazon.json"]
    r1=sc.textFile('wallmart.json');
    r2=sc.textFile('amazon.json');

   
    rdd=r1.union(r2)
    r1=r1.map(parseclean)
    print(r1.take(2))
    r2=r2.map(parseclean)
    rdd1=r1.union(r2)

   
    

    
    ## cleaning the input to a type=> (title,(priceWhole,rating,totalPeopleRated,imageUrl))
    cleanRdd = rdd.map(parseInput)
    
    ## creating offset for individual line =>(1,firstLine)
    cleanRddWithIndex=cleanRdd.zipWithIndex()
     
    ## creaing index with title=>(index,title)
    cleanRddWithIndex=cleanRddWithIndex.map(lambda x: (x[1],x[0]))

    

   
    fclean = open("CleanInput.json", "a+")
    #file2=sc.parallelize(TopRecomendation)
    fclean.write("[ \n")
    for x in rdd1.collect():
        fclean.write(str(x)+"\n")
    #fclean.write(",{} \n ]")
    with open("CleanInput.json", "a") as fclean1:
        fclean1.write(",{}\n]")
   
    ## reverse =>(title,index)
    #toIndexOne=cleanRddWithIndex.map(lambda x :comput(x[1][0],x[0]))
    toIndexOne=cleanRddWithIndex.map(lambda x :comput(x[1][0],x[0]))
    toIndexBi=cleanRddWithIndex.map(lambda x :computBi(x[1][0],x[0]))
    toIndex=toIndexOne.union(toIndexBi)
    print(toIndex.collect())
    #file2=sc.parallelize(TopRecomendation)
    
  
    
   
    ## grouping based on key => (key,[offset])
    toIndex= toIndex.sortByKey();
    toIndex= toIndex.combineByKey(lambda v:[v],lambda x,y:x+[y],lambda x,y:x+y)
    #toIndex.saveAsTextFile("Index.txt")
    dfIndex =spark.createDataFrame(toIndex)
    index=dfIndex.toJSON()
    fIndex = open("Index.json", "a+")
    #file2=sc.parallelize(TopRecomendation)
    fIndex.write("[ \n")
    for x in index.collect():
        fIndex.write(str(x)+",\n")
    with open("Index.json", "a") as fIndex1:
        fIndex1.write("{}]")
    
    ## getting top 100 recomendation
    recomendationRdd=cleanRdd
    calculateRecomendation=recomendationRdd.map(calculate)
    TopRecomendation=calculateRecomendation.sortBy(lambda x:-x[1])
    TopRecomendation=TopRecomendation.take(100)
    #TopRecomendation.saveAsTextFile("TopRecomendation.txt")
    dfTopRecomendation =spark.createDataFrame(TopRecomendation)
    g=dfTopRecomendation.toJSON()

    
    frec = open("TopRecomendationjson.json", "a+")
    #file2=sc.parallelize(TopRecomendation)
    frec.write("[\n")
    for x in g.collect():
        frec.write(str(x)+",\n")
    with open("TopRecomendationjson.json", "a") as myfile:
        myfile.write("{}]")
  


    
    #print(TopRecomendation.take(10))
    sc.stop();
