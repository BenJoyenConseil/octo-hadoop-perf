octo-hadoop-perf
================
Améliorer la performance des jobs Hadoop sur HDInsight

Repo de code map reduce pour Hadoop YARN qui s'execute sur les logs Wikipedia http://dumps.wikimedia.org/other/pagecounts-raw/

Pour lancer l'application sur Hadoop 2.2, installez Gradle 1.2 puis executez la commande suivante :

    cd /home/usr/mon_dossier_de_travail/hadoop-mr/
    gradle build

Ensuite, une archive jar a été produite dans la racine du dossier hadoop-mr : wikipedia.jar

Utilisez votre cluster Hadoop avec la commande suivante :

    hadoop jar wikipedia.jar -date 20140625

Une option est necessaire :

   -date : precise la date à laquelle l'analyse commence (ex : 20140514) au format [yyyyMMdd].
