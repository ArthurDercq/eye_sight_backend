1. Objectif principal :

Créer une application personnelle qui centralise tes données d’activités sportives (Strava), pour :

les stocker localement dans une base PostgreSQL,

les analyser et visualiser sous forme de graphiques personnalisés,

produire des posters visuels (cartes, profils de dénivelé, heatmaps, etc.) de tes activités.

En bref : un tableau de bord sportif personnalisé, connecté à Strava mais totalement sous ton contrôle.



2. Architecture actuelle

--> Backend (déjà opérationnel)

Framework : FastAPI

Langage : Python

Organisation en modules clairs :

routers/ : endpoints FastAPI (activities, plots, kpi, strava…)

services/ : logique métier (accès base, traitement données, génération de profils)

db/ : gestion connexion PostgreSQL (psycopg2, sqlalchemy)

plots/ : génération d’images (cartes, profils, heatmaps, art)

API disponible localement sur http://localhost:8000

--> Frotnend


3. Base de données

PostgreSQL

Tables principales :

activites → données globales d’une activité Strava (distance, D+, date, vitesse, type, etc.)

streams → données de trace détaillée (coordonnées, altitude, distance_m, time_s)

Stockage mis à jour via l’API Strava (fetch, clean, insert)

4. Étapes prévues par CLaude Code

Créer un frontend séparé

Sera dans un dossier eye_sight_front

Doit consommer l’API du backend

Objectif initial : afficher tes activités et générer des visuels (posters de D+ notamment)

Technologies à utiliser :

Windsurf (Python) : simple, rapide à mettre en place

Le frontend sera lancé dans un conteneur Docker séparé.

5. Containerisation complète

docker-compose.yml avec deux services :

backend : FastAPI

frontend : app Windsurf

La base PostgreSQL peut rester locale (hors Docker) dans un premier temps.


⚠️ Contraintes identifiées

Tu veux que backend et frontend soient deux unités indépendantes

Tu veux que l’appli soit privée (usage perso), donc pas de gestion utilisateurs ni authentification complexe

Performance importante : éviter de charger tous les streams si pas nécessaire

API doit renvoyer des données brutes JSON (pas de génération d’images dans le backend)

Besoin de flexibilité pour combiner plusieurs sport_type dans les filtres

Base déjà existante, donc le backend doit pouvoir accéder à PostgreSQL locale même en étant dans un conteneur → utilisation de host.docker.internal


🧭 Vision finale

Un dashboard personnel avec :

  - cartes interactives de tes parcours

  - courbes de dénivelé cumulées

  - statistiques agrégées (distance, D+, temps…)

  - génération de posters artistiques de tes traces sportives
