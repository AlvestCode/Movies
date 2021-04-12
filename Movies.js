//1.Analizar con find la colección.
db.movies.find()

//2.Contar cuantos documentos tiene cargado.
db.movies.find().count()

//3.Insertar una película.
db.movies.insertOne( { title: "The Hateful Eight", year: NumberInt(2015) , cast: ["Samuel L. Jackson"], genres: ["Western"] } )

//4.Borrar esa película.
db.movies.deleteOne( {title: "The Hateful Eight"} )

//5.Sacar cuantas películas tienen actores llamados and (están por error).
db.movies.find( { cast: "and" } ).count()

//6.Actulizar los documentos cuyo cast sea valor "and" sacando fuera las entradas del array, 
//no se debe eliminar ni el documentto ni el array. 
db.movies.updateMany( { cast: "and" }, { $pull: { cast: "and" } } )

//7.Contar cuantos documentos no tienen actores.
db.movies.find( { cast: { $size: 0 } } ).count()

//8.Actualizar TODOS  que no tengan actores, metiendo un nuevo elemento dentro del array 
//con valor Undefined.-> ["Undefined"].
db.movies.updateMany( { cast: { $size: 0 } }, { $set: { cast: ["Undefined"] } } )

//9.Contar cuantos documentos no tienen género.
db.movies.find( { genres: { $size: 0 } } ).count()

//10.Actualizar TODOS  que no tengan géneros, metiendo un nuevo elemento dentro del array 
//con valor Undefined.-> ["Undefined"].
db.movies.updateMany( { genres: { $size: 0 } }, { $set: { genres: ["Undefined"] } } )

//11.Sacar el año más reciente / actual.
db.movies.find({}, {_id: 0, year: 1 }).sort( { year: -1 } ).limit(1)

//12.Contar cuántas películas han salido en los últimos 20 años 
//desde el último año que tiene registradas películas (aggregate).
db.movies.aggregate( [ { $group: { _id: "$year", pelis: { $sum: 1 } } },
                       { $sort: { _id: -1 } },
                       { $limit: 20 },
                       { $group: { _id: null, total: { $sum: "$pelis" } } } ] )                       

//13.Contar cuántas películas han salido en la década de los 60 (del 60 al 69 incluidos) (aggregate).
db.movies.aggregate( [ { $match: { year: { $gte: 1960, $lte: 1969 } } },
                       { $group: { _id: null, total: { $sum: 1 } } } ] )

//14.Sacar el año con más películas.
db.movies.aggregate( [ { $group: { _id: "$year", Total: { $sum: 1 } } },
                       { $sort: { Total: -1 } },
                       { $limit: 1 } ] )


//15.Sacar el año con menos películas.
db.movies.aggregate( [ { $group: { _id: "$year", Total: { $sum: 1 } } },
                       { $sort: { Total: 1 } },
                       { $limit: 1 } ] )

//16.Guardar en nueva colección actors realizando unwind por actor y después, una nueva consulta
//para contar cuantos documentos existen en la nueva colección.
db.movies.aggregate( [ { $unwind: "$cast" },
                       { $project: { "_id": 0 } },
                       { $out: "actors" } ] )

db.actors.find().count()


//17.Sobre actors (nueva colección), sacar lista top de los actores que han participado en más películas,
//filtrando para descartar aquellos que sean "Undefined", no se eliminan de la colección, 
//sólo que filtramos para que no aparezcan.
db.actors.aggregate( [ { $match: { cast: { $ne: "Undefined" } } },
                       { $group: { _id: "$cast", cuenta: { $sum: 1 } } },
                       { $sort: { cuenta: -1 } },
                       { $limit: 1 } ] ) 

//18.Sobre actors (nueva colección), sacar la película y año que más actores han participado.
db.actors.aggregate( [ { $group: { _id: {title: "$title", year: "$year"}, cast: { $addToSet: "$cast" } } }, 
                       { $project: { title: 1, year: 1, cuenta:{ $size: "$cast" }  } },
                       { $sort: { cuenta : -1 } },
                       { $limit: 1 } ] )

//19.Sobre actors (nueva colección), sacar los actores cuya carrera haya sido la más larga,
//filtrando para descartar aquellos que sean "Undefined", no se eliminan de la colección, 
//sólo que filtramos para que no aparezcan.
db.actors.aggregate( [ { $match: { cast: { $ne: "Undefined" } } },
                       { $group: { _id: "$cast", year: { $addToSet: "$year" } } },
                       { $project: { cast: 1, comienza: { $min: "$year" }, termina: { $max: "$year" }, 
                          annos: { $subtract: [ { $max: "$year" }, { $min: "$year" } ] } } },
                       { $sort: { annos: -1 } },
                       { $limit: 1 } ] )

//20.Sobre actors (nueva colección), guardar en nueva colección genres realizando unwind por genres y después,
//una nueva consulta para contar cuantos documentos existen en la nueva colección.
db.actors.aggregate( [ { $unwind: "$genres" },
                       { $project: { "_id": 0 } },
                       { $out: "genres" } ] )

db.genres.find().count()

//21.Sobre genres (nueva colección), sacar por Año y Género de mayor a menor el número de películas.
db.genres.aggregate( [ { $group: { _id: { year: "$year", genres: "$genres"}, title: { $addToSet: "$title"  } } },
                       { $project: { year: 1, genres: 1, pelis: { $size: "$title" } } },
                       { $sort: { pelis: -1 } } ] )

//22.Sobre genres (nueva colección), sacar el top actores que han participado en más números de géneros,
//filtrando para descartar aquellos que sean "Undefined", no se eliminan de la colección, sólo que filtramos para que no aparezcan.
db.genres.aggregate( [ { $match: { cast: { $ne: "Undefined" } } },
                       { $group: { _id: "$cast", genres: { $addToSet: "$genres"  } } },
                       { $project: { cast: 1, numgeneros: { $size: "$genres" }, generos: "$genres" } },
                       { $sort: { numgeneros: -1 } },
                       { $limit: 1 } ] )

//23.Sobre genres (nueva colección), sacar película y su año que más géneros tiene.
db.genres.aggregate( [ { $group: { _id: { title: "$title", year: "$year" }, genres: { $addToSet: "$genres"  } } },
                       { $project: { title: 1, numgeneros: { $size: "$genres" }, generos: "$genres" } },
                       { $sort: { numgeneros: -1 } },
                       { $limit: 1 } ] )

//24.Query libre sobre agregación. Año con más películas de comedia 
db.movies.aggregate( [ { $match: { genres: "Comedy" } },
                       { $group: { _id: "$year", pelis: { $sum: 1 } } },
                       { $sort: { pelis: -1 } },
                       { $limit: 1 } ] )

//25.Query libre sobre agregación. Media anual películas de suspense (Desde el año de estreno de la primera a la última)
db.genres.aggregate( [ { $match: { genres: "Suspense" } },
                       { $group: { _id: "$genres", pelis: { $sum: 1 }, year: { $addToSet: "$year" } } },
                       { $project: { _id: 1, pelis: 1, annos: { $subtract: [ { $max: "$year" }, { $min: "$year" } ] } } },
                       { $project: { _id: 0, genero: "$_id", mediaPelis: { $round: [ { $divide: [ "$pelis", "$annos" ] }, 2] } } } ] )

//26.Query libre sobre agregación. Actor con más películas de Western.
db.actors.aggregate( [ { $match: {cast: { $ne: "Undefined" } ,genres: "Western" } },
                       { $group: { _id: "$cast", pelis: { $sum: 1 } } },
                       { $sort: { pelis: -1 } },
                       { $limit: 1 } ] )
                        