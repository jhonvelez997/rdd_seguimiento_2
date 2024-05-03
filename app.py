import findspark
from pyspark.sql import SparkSession
from flask import Flask,request,render_template, jsonify, url_for
import json


findspark.init()
spark = SparkSession.builder.appName("Api_seguimiento").master("local[*]").getOrCreate()
sc = spark.sparkContext 



ratings_rdd = sc.textFile("ratings.json")
ratings_rdd= ratings_rdd \
    .map(lambda x: x.split(":")) \
    .map(lambda x : [x[1].split(",")[0] , x[3].replace("}","")]) \
    .map(lambda x: [c.strip() for c in x]) \
    .map(lambda x: [x[0], int(x[1])]) \
    .mapValues(lambda x: (x,1)) \
    .reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1])) \
    .mapValues(lambda x : (round((x[0]/x[1]),2), x[1]))

# -------------------------------------------

titles = {}
with open("metadata.json", encoding="utf-8") as file:
    for line in file:
        json_data = json.loads(line)    
        titles[str(json_data["item_id"])] = (
           json_data["title"],
           json_data["authors"],
           json_data["year"]
        )
titles_boradcast = sc.broadcast(titles)

# -------------------------------------------

full_rdd = ratings_rdd \
    .map(lambda x : (x[0], x[1][0], x[1][1]) + titles_boradcast.value[x[0]] ) \
    .map(lambda x :(x[0], x[1], x[2], x[3],x[4].lower(), int(x[5])) )

app = Flask(__name__)

@app.route("/")
def home():
    return  render_template("home_index.html")

@app.route("/book_by_rating")
def book_by_rating():
    min_ = request.args.get("rating_min")
    max_ = request.args.get("rating_max")

    if min_ == None and max_ == None:
        return jsonify({"Mensaje":"No pasaste ninguno de los argumentos"})
    elif min_ != None and max_ == None:
        try:
            min_ = float(min_.replace(",","."))
        except:
            return jsonify({"Mensaje":f"El argumento que ingresaste no es númerico: {min_} "})
        if min_ >= 0 and min_ <= 5:
            res = full_rdd \
                .filter(lambda x : x[1] >= min_) \
                .sortBy(lambda x: -x[1] ) \
                .collect()
            return jsonify({"Response":res})
        else:
            return jsonify({"Mensaje":f"El argumento que ingresaste para rating_min : {min_} no esta en el rango de 0 a 5"})
    elif min_ == None and max_ !=None:
        try:
            max_ = float(max_.replace(",","."))
        except:
            return jsonify({"Mensaje":f"El argumento que ingresaste no es númerico: {max_} "})
        if max_ >= 0 and max_ <= 5:
            res = full_rdd \
                .filter(lambda x : x[1] <= max_) \
                .sortBy(lambda x: -x[1] ) \
                .collect()
            return jsonify({"Response":res})
        else:
            return jsonify({"Mensaje":f"El argumento que ingresaste para rating_max : {max_} no esta en el rango de 0 a 5"})
    elif min_ != None  and max_ != None :
        try:
            max_ = float(max_.replace(",","."))
        except:
            return jsonify({"Mensaje":f"El argumento que ingresaste no es númerico: {max_} "})
        try:
            min_ = float(min_.replace(",","."))
        except:
            return jsonify({"Mensaje":f"El argumento que ingresaste no es númerico: {min_} "})
        if min_ > max_:
            return jsonify({"Mensaje":"rating_max debe ser mayor que rating_min"})
        if max_ >= 0 and max_ <= 5 and min_ >= 0 and min_ <= 5:
            res = full_rdd \
                .filter(lambda x : x[1] <= max_ and x[1] >= min_) \
                .sortBy(lambda x: -x[1] ) \
                .collect()
            return jsonify({"Response":res})
        else:
            return jsonify({"Mensaje":"Los valores de rating_min y rating_max deben estar en  el rango de 0 a 5, revisa por favor "})



@app.route("/authors")
def authors():
    res = full_rdd \
        .map(lambda x: x[-2]) \
        .distinct() \
        .collect()
    return jsonify(res)



@app.route("/book_by_author")
def book_by_author():
    author = request.args.get("author")
    if author != None:
        author = author.lower().strip()
        print(author)
        res = full_rdd \
            .filter(lambda x: author in x[-2]).sortBy(lambda x: -x[1] ).collect()
        if len(res) <= 0:
            return jsonify({
                "Mensaje": f"el parametro {author} no retorno ningun libro,  Revisa el endpoint /authors este contiene la lista con todos los autores"
                })
        else:
            return jsonify(res)
    else:
        return jsonify({"Mensaje":"No pasaste el parametro 'author' a la busqueda"})


@app.route("/book_by_year")
def book_by_year():
    yr = request.args.get("year")

    if yr != None:
        try:
            yr = int(yr.replace(",","").replace(".",""))
        except:
            return jsonify(
                {
                    "Mensaje": f"El argumento de busqueda ingresado {yr} no es numerico"
                }
            )        
        res = full_rdd \
            .filter(lambda x: x[-1] == yr ).sortBy(lambda x: -x[1] ).collect()
        
        if len(res) == 0:
            return jsonify({"Mensaje":"Nohay libros en el año seleccionado"})
        else:
            return jsonify(res)
    else:
        return jsonify(
            {
                "Mesnaje": "No ingresaste el parametro de busqueda year"
            }
        )




@app.route("/get_book")
def get_book():
    yr = request.args.get("year")
    author = request.args.get("author")
    min_ = request.args.get("rating_min")
    max_ = request.args.get("rating_max")

    if yr == None and author == None and min_ == None and max_ == None:
        return jsonify({"Mensaje":"No pasaste ningun argumento de busqueda"})
    
    elif yr != None and author == None and min_ == None and max_ == None:
        try:
            yr = int(yr.replace(",","").replace(".",""))
        except:
            return jsonify(
                {
                    "Mensaje": f"El argumento de busqueda ingresado {yr} no es numerico"
                }
            ) 
        res = full_rdd \
            .filter(lambda x: x[-1] == yr ).collect()
        if len(res) == 0:
            return jsonify({"Mensaje":"No hay libros en el año seleccionado"})
        else:
            return jsonify(res)

    elif yr != None and author != None and min_ == None and max_ == None:
        try:
            yr = int(yr.replace(",","").replace(".",""))
        except:
            return jsonify(
                {
                    "Mensaje": f"El argumento de busqueda ingresado {yr} no es numerico"
                }
            )
        author = author.lower().strip()
        res = full_rdd \
            .filter(lambda x: author in x[-2]) \
            .filter(lambda x: x[-1] == yr ) \
            .sortBy(lambda x: -x[1] ).collect()
        if len(res) == 0:
            return jsonify({"Mensaje":"No hay libros con los argumentos de busqueda utilizados"})
        else:
            return jsonify(res)
        
    elif yr != None and author != None and min_ != None and max_ == None:
        try:
            yr = int(yr.replace(",","").replace(".",""))
        except:
            return jsonify(
                {
                    "Mensaje": f"El argumento de busqueda ingresado {yr} no es numerico"
                }
            )
        author = author.lower().strip()
        try:
            min_ = float(min_.replace(",","."))
        except:
            return jsonify({"Mensaje":f"El argumento que ingresaste para min_rating no es númerico: {min_} "})  
        if min_ >= 0 and min_ <= 5:
            res = full_rdd \
                .filter(lambda x : x[1] >= min_) \
                .filter(lambda x: author in x[-2]) \
                .filter(lambda x: x[-1] == yr ) \
                .sortBy(lambda x: -x[1] ) \
                .collect()
            
            if len(res) == 0:
                return jsonify({"Mensaje":f"No hay libros con los argumentos de busqueda pasados"})
            else:
                return jsonify({"Response":res})
        else:
            return jsonify({"Mensaje":f"El argumento que ingresaste para rating_min : {min_} no esta en el rango de 0 a 5"})
        

    elif yr != None and author != None and min_ != None and max_ != None:
        try:
            yr = int(yr.replace(",","").replace(".",""))
        except:
            return jsonify(
                {
                    "Mensaje": f"El argumento de busqueda ingresado {yr} no es numerico"
                }
            )
        author = author.lower().strip()
        try:
            max_ = float(max_.replace(",","."))
        except:
            return jsonify({"Mensaje":f"El argumento que ingresaste para rating_max no es númerico: {max_} "})
        try:
            min_ = float(min_.replace(",","."))
        except:
            return jsonify({"Mensaje":f"El argumento que ingresaste para rating_min no es númerico: {min_} "})
        if min_ > max_:
            return jsonify({"Mensaje":"rating_max debe ser mayor que rating_min"})
        if max_ >= 0 and max_ <= 5 and min_ >= 0 and min_ <= 5:
            res = full_rdd \
                .filter(lambda x : x[1] <= max_ and x[1] >= min_) \
                .sortBy(lambda x: -x[1] ) \
                .filter(lambda x: author in x[-2]) \
                .filter(lambda x: x[-1] == yr ) \
                .collect()
            
            if len(res) <= 0:
                return jsonify({"Mensaje":"No hay libros encontrados de acuerdo a los argumentos de busqueda"})
            return jsonify({"Response":res})
        else:
            return jsonify({"Mensaje":"Los valores de rating_min y rating_max deben estar en  el rango de 0 a 5, revisa por favor "})
        
    elif yr == None and author != None and min_ == None and max_ == None:
        author = author.lower().strip()
        res = full_rdd \
            .filter(lambda x: author in x[-2]).sortBy(lambda x: -x[1] ).collect()
        if len(res) <= 0:
            return jsonify({
                "Mensaje": f"el parametro {author} no retorno ningun libro,  Revisa el endpoint /authors este contiene la lista con todos los autores"
                })
        else:
            return jsonify(res)
        
    elif yr == None and author != None and min_ != None and max_ == None:
        author = author.lower().strip()
        try:
            min_ = float(min_.replace(",","."))
        except:
            return jsonify({"Mensaje":f"El argumento que ingresaste para min_rating no es númerico: {min_} "})  
        if min_ >= 0 and min_ <= 5:
            res = full_rdd \
                .filter(lambda x : x[1] >= min_) \
                .filter(lambda x: author in x[-2]) \
                .sortBy(lambda x: -x[1] ) \
                .collect()
            if len(res) <= 0:
                return jsonify({"Mensaje":"Tu busqueda no retorno resultados"})
            else:
                return jsonify(res)
        else :
            return jsonify({"Mensaje":"El min rating debe estar contenido entre 0 y 5 "})


    elif yr == None and author != None and min_ != None and max_ != None:
        author = author.lower().strip()
        try:
            min_ = float(min_.replace(",","."))
        except:
            return jsonify({"Mensaje":f"El argumento que ingresaste para min_rating no es númerico: {min_} "})
        try:
            max_ = float(max_.replace(",","."))
        except:
            return jsonify({"Mensaje":f"El argumento que ingresaste para rating_max no es númerico: {max_} "})  
        if min_ > max_:
            return jsonify({"Mensaje":"rating_max debe ser mayor que rating_min"})
        if max_ >= 0 and max_ <= 5 and min_ >= 0 and min_ <= 5:
            res = full_rdd \
                .filter(lambda x : x[1] <= max_ and x[1] >= min_) \
                .sortBy(lambda x: -x[1] ) \
                .filter(lambda x: author in x[-2]) \
                .collect()
            return jsonify({"Response":res})
        else:
            return jsonify({"Mensaje":"Los valores de rating_min y rating_max deben estar en  el rango de 0 a 5, revisa por favor "})
        
    elif yr == None and author != None and min_ == None and max_ != None:
        author = author.lower().strip()
        try:
            max_ = float(max_.replace(",","."))
        except:
            return jsonify({"Mensaje":f"El argumento que ingresaste para rating_max no es númerico: {max_} "})  

        if max_ >= 0 and max_ <= 5:
            res = full_rdd \
                .filter(lambda x : x[1] <= max_) \
                .sortBy(lambda x: -x[1] ) \
                .filter(lambda x: author in x[-2]) \
                .collect()
            return jsonify({"Response":res})
        else:
            return jsonify({"Mensaje":"Los valores de rating_min y rating_max deben estar en  el rango de 0 a 5, revisa por favor "})

    elif yr == None and author == None and min_ == None and max_ != None:
        try:
            max_ = float(max_.replace(",","."))
        except:
            return jsonify({"Mensaje":f"El argumento que ingresaste para rating_max no es númerico: {max_} "}) 
        if max_ >= 0 and max_ <= 5:
            res = full_rdd \
                .filter(lambda x : x[1] <= max_) \
                .sortBy(lambda x: -x[1] ) \
                .collect()
            return jsonify({"Response":res})
        else:
            return jsonify({"Mensaje":"Los valores de rating_min y rating_max deben estar en  el rango de 0 a 5, revisa por favor "})

    elif yr == None and author == None and min_ != None and max_ == None:
        try:
            min_ = float(min_.replace(",","."))
        except:
            return jsonify({"Mensaje":f"El argumento que ingresaste para rating_max no es númerico: {min_} "}) 
        if min_ >= 0 and min_ <= 5:
            res = full_rdd \
                .filter(lambda x : x[1] >= min_) \
                .sortBy(lambda x: -x[1] ) \
                .collect()
            return jsonify({"Response":res})
        else:
            return jsonify({"Mensaje":"Los valores de rating_min y rating_max deben estar en  el rango de 0 a 5, revisa por favor "})


    elif yr != None and author == None and min_ != None and max_ == None:
        try:
            yr = int(yr.replace(",","").replace(".",""))
        except:
            return jsonify(
                {
                    "Mensaje": f"El argumento de busqueda ingresado {yr} no es numerico"
                }
            )
        try:
            min_ = float(min_.replace(",","."))
        except:
            return jsonify({"Mensaje":f"El argumento que ingresaste para rating_min no es númerico: {min_} "})
        if min_ >= 0 and min_ <= 5:
            res = full_rdd \
                .filter(lambda x : x[1] >= min_) \
                .filter(lambda x: x[-1] == yr ) \
                .sortBy(lambda x: -x[1] ) \
                .collect()
            if len(res) <= 0:
                return  jsonify({"Mensaje":"No hay libros para de acuerdo a los argumetos de busqueda pasados"})
            return jsonify({"Response":res})
        else:
            return jsonify({"Mensaje":"Los valores de rating_min y rating_max deben estar en  el rango de 0 a 5, revisa por favor "})
        

    elif yr != None and author == None and min_ == None and max_ != None:
        try:
            yr = int(yr.replace(",","").replace(".",""))
        except:
            return jsonify(
                {
                    "Mensaje": f"El argumento de busqueda ingresado {yr} no es numerico"
                }
            )
        try:
            max_ = float(max_.replace(",","."))
        except:
            return jsonify({"Mensaje":f"El argumento que ingresaste para rating_max no es númerico: {max_} "})
        if max_ >= 0 and max_ <= 5:
            res = full_rdd \
                .filter(lambda x : x[1] <= max_) \
                .filter(lambda x: x[-1] == yr ) \
                .sortBy(lambda x: -x[1] ) \
                .collect()
            if len(res) <= 0:
                return  jsonify({"Mensaje":"No hay libros para de acuerdo a los argumetos de busqueda pasados"})
            return jsonify({"Response":res})
        else:
            return jsonify({"Mensaje":"Los valores de rating_min y rating_max deben estar en  el rango de 0 a 5, revisa por favor "})
        
    elif yr == None and author == None and min_ != None and max_ != None:
        try:
            max_ = float(max_.replace(",","."))
        except:
            return jsonify({"Mensaje":f"El argumento que ingresaste no es númerico: {max_} "})
        try:
            min_ = float(min_.replace(",","."))
        except:
            return jsonify({"Mensaje":f"El argumento que ingresaste no es númerico: {min_} "})
        if min_ > max_:
            return jsonify({"Mensaje":"rating_max debe ser mayor que rating_min"})
        if max_ >= 0 and max_ <= 5 and min_ >= 0 and min_ <= 5:
            res = full_rdd \
                .filter(lambda x : x[1] <= max_ and x[1] >= min_) \
                .sortBy(lambda x: -x[1] ) \
                .collect()
            return jsonify({"Response":res})
        else:
            return jsonify({"Mensaje":"Los valores de rating_min y rating_max deben estar en  el rango de 0 a 5, revisa por favor "})
        
    elif yr != None and author != None and min_ == None and max_ != None:
        author = author.lower().strip()
        try:
            yr = int(yr.replace(",","").replace(".",""))
        except:
            return jsonify(
                {
                    "Mensaje": f"El argumento de busqueda ingresado {yr} no es numerico"
                }
            )
        try:
            max_ = float(max_.replace(",","."))
        except:
            return jsonify({"Mensaje":f"El argumento que ingresaste para rating_max no es númerico: {max_} "})
        if max_ >= 0 and max_ <= 5:
            res = full_rdd \
                .filter(lambda x : x[1] <= max_) \
                .filter(lambda x: author in x[-2]) \
                .filter(lambda x: x[-1] == yr ) \
                .sortBy(lambda x: -x[1] ) \
                .collect()
            if len(res) <= 0:
                return  jsonify({"Mensaje":"No hay libros para de acuerdo a los argumetos de busqueda pasados"})
            return jsonify({"Response":res})
        else:
            return jsonify({"Mensaje":"Los valores de rating_min y rating_max deben estar en  el rango de 0 a 5, revisa por favor "})
        

    elif yr != None and author == None and min_ != None and max_ != None:
        try:
            yr = int(yr.replace(",","").replace(".",""))
        except:
            return jsonify(
                {
                    "Mensaje": f"El argumento de busqueda ingresado {yr} no es numerico"
                }
            )
        try:
            max_ = float(max_.replace(",","."))
        except:
            return jsonify({"Mensaje":f"El argumento que ingresaste no es númerico: {max_} "})
        try:
            min_ = float(min_.replace(",","."))
        except:
            return jsonify({"Mensaje":f"El argumento que ingresaste no es númerico: {min_} "})
        if min_ > max_:
            return jsonify({"Mensaje":"rating_max debe ser mayor que rating_min"})
        if max_ >= 0 and max_ <= 5 and min_ >= 0 and min_ <= 5:
            res = full_rdd \
                .filter(lambda x : x[1] <= max_ and x[1] >= min_) \
                .filter(lambda x: x[-1] == yr ) \
                .sortBy(lambda x: -x[1] ) \
                .collect()
            return jsonify({"Response":res})
        else:
            return jsonify({"Mensaje":"Los valores de rating_min y rating_max deben estar en  el rango de 0 a 5, revisa por favor "})
        
    else:
        return jsonify({"Mesnaje":"No hay datos para esta combinacion de parametros"})



app.run(host="0.0.0.0", port=9191, debug=True)
