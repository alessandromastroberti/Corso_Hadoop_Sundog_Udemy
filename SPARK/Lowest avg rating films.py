from pyspark import SparkConf, SparkContext


'''
questa funzione legge file contenente informazioni sui film 
e crea un dizionario dove gli ID dei film sono le chiavi e i nomi dei film sono i valori corrispondenti. 
Questo dizionario può essere utilizzato per accedere rapidamente al nome di un film dato il suo ID.
'''

def loadMovieNames():
    movieNames = {}
    with open('ml-100k/u.item') as f:
        for line in f:
            fields = line.split('|') #restituisce lista di valori separati da |
            movieNames[int(fields[0])] = fields[1]
    return movieNames

def parseInput(line):
    fields = line.split()
    return (int(fields[1], (float(fields[2]), 1.0)))

if __name__ == '__main__':
    #cofiguro nome app e contesto di spark
    conf = SparkConf().setAppName('WorstMovies')
    sc = SparkContext(conf=conf)

    #carico il dizionario
    movieNames = loadMovieNames()

    #carico file u.data che viene letto come un RDD (Resilient Distributed Dataset) di righe.
    lines = sc.textFile('hdfs:///user/maria_dev/ml-100k/u.data')

    #converto in (movieID, (rating, 1.0))
    movieRatings = lines.map(parseInput)

    #I dati vengono ridotti usando reduceByKey() per calcolare la somma totale delle valutazioni e il conteggio totale delle valutazioni per ciascun movieID. 
    #Questo risultato è rappresentato come un RDD di tuple nel formato (movieID, (sumOfRatings, totalRatings)).
    ratingTotalsAndCount = movieRatings.reduceByKey(lambda movie1, movie2: ( movie1[0] + movie2[0], movie1[1] + movie2[1] ) )

    # Vengono calcolate le valutazioni medie dei film dividendo la somma totale delle valutazioni per il conteggio totale delle valutazioni per ciascun movieID. 
    # Questo risultato è rappresentato come un RDD di tuple nel formato (movieID, averageRating).
    # La funzione mapValues() applica la lambda function fornita solo ai valori (non alle chiavi) di ogni coppia chiave-valore nell'RDD.
    averageRatings = ratingTotalsAndCount.mapValues(lambda totalAndCount : totalAndCount[0] / totalAndCount[1])

    '''
    il codice utilizza Apache Spark per caricare un dizionario dei nomi 
    dei film e calcolare le valutazioni medie per ciascun film utilizzando i dati del file "u.data". 
    Il risultato finale è un RDD contenente il movieID e la rispettiva valutazione media per ogni film presente nel dataset.
    '''

    sortedMovies = averageRatings.sortBy(lambda x: x[1])

    results = sortedMovies.take(10)

    for result in results: 
        print(movieNames[result[0]], result[1])

