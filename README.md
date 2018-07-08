# SocialNetworkSPARK

Social Network using SPARK Dataset

Le but de ce projet est de faire un réseau social en utilisant SPARK en SCALA.

Nous avons donc implémenter ce réseau contenant des utilisateurs qui peuvent envoyer des messages, faire des posts, poster leur localisation et liker des posts.

Pour cela nous stockons nos données dans Cassandra, et nous mettons ces donnée dans des fichiers HDFS toutes les heures.

Nous avons crée une API pour pouvoir ingérer les données dans kafka directement depuis l'API.
