# SocialNetworkSPARK

Social Network using SPARK Dataset

Le but de ce projet est de faire un réseau social en utilisant SPARK en SCALA.

Nous avons donc implémenter ce réseau contenant des utilisateurs qui peuvent envoyer des messages, faire des posts, poster leur localisation et liker des posts.

Pour cela nous stockons nos données dans Cassandra, et nous mettons ces donnée dans des fichiers HDFS toutes les heures.

Nous avons crée une API pour pouvoir ingérer les données dans kafka directement depuis l'API.

Documentation des  API  REST #####:

Le point d'entrée est /api

Location Partie:

GET /location/id?q={locationId}
Retourne la location associé à l'id

POST /location
Lance un producer Location

Message Partie:

GET /message/id?q={messageId}
Retourne le message associé à l'id

POST /message
Lance un producer Message

User Partie:

GET /user/id?q={userEmail}
Retourne l'utilisateur associé à l'email

POST /user
Lance un producer User

Like Partie:

GET /like/id?q={likeId}
Retourne le like associé à l'id

POST /like
Lance un producer Like

Post Partie:

GET /post/id?q={likeId}
Retourne le post associé à l'id

POST /post
Lance un producer Post

Partie Recherche:

Le {dateFrom} est au format yyyy-MM-dd

GET /search/date/from?q={dateFrom}&brand={brand}
Retourne tout les messages et post faisant référence à la marque {brand} à partir de la date {dateFrom}

GET /search/date/to?q={dateFrom}&brand={brand}
Retourne tout les messages et post faisant référence à la marque {brand} avant la date {dateFrom}

GET /search/date/any?from={dateFrom}&to={to}&brand={brand}
Retourne tout les messages et post faisant référence à la marque {brand} à partir de la date {dateFrom} jusqu'à la date
{to}
GET /search?brand={brand}
Retourne tout les messages et post faisant référence à la marque {brand}


----Example----:

Faire une recherche de message et topic sur la marque Adidas:

$ curl -X GET http://localhost:8080/api/search?brand=adidas
