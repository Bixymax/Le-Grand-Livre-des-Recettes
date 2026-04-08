# UE08 Big Data - Running Spark Locally using Docker Compose


Based on [karl chris/spark-docker](https://karlchris.github.io/data-engineering/projects/spark-docker/).

# Général

Les instructions ci-dessous permettent de mettre en place le cluster Spark utilisé pour le cours de Big Data.

Il sera nécessaires de les exécuter une première fois pour construire les images Docker sur votre machine mais également
dans le cas où vos containers ont cessé de fonctionner.

Plusieurs containers seront créés afin de simuler les nodes du cluster

Les instructions de la section "Redémarrage" 

# Setup


**1. Démarrer le cluster en lançant la commande suivante depuis la racine de ce projet.**
```bash
docker-compose up
```

**Note** : La première fois que vous lancerez la commande, cette commande s'occupera également de télécharger/construire les images docker.

**2. Connection interactive vers le master node**
```bash
docker exec -it spark-master /bin/bash
```
❓ Que fait cette commande ?

**3. Lancer PySpark** 
```bash
# shell of the pyspark master node in docker container
pyspark
```

**4. Accès au serveur Jupyter installé sur le master node.**

[Option #1] Ce lien va apparaître dans la console. Cliquez dessus pour accéder à l'interface web.
```
http://127.0.0.1:8889/tree?token=...
```

[Option #2] Connection via Pycharm : Parametres --> Jupyter --> Créer un nouveau serveur jupyter et y mettre :

    Lien: http://localhost:8889
    Token : [Celui dans la console]

**Note** : Tous les fichiers de ce projet sont montés dans le master node.

❓ Qu'est-ce que ça veut dire ?

Cluster services are now available locally in the host machine browser:
- http://localhost:8080 - WebUI of the master node.
    - Workers must be visible and accessible.
    - The current jupyter process should be represented as an active *PySparkSession*. It should be assigned workers and cores.
- http://localhost:18080 - history-node.
    - The history of jobs.
    - View logs and statistics on resources for each launch of the *Application* (*SparkSession*).
