# Guide de Migration - Ajout de Nouveaux Streams

Ce guide explique comment ajouter les nouvelles données de streams (heartrate, cadence, velocity_smooth, temp, power, grade_smooth) à votre base de données existante sans perdre vos données.

## Vue d'ensemble

Cette migration ajoute 6 nouvelles colonnes à la table `streams`:
- **heartrate** (INTEGER): Fréquence cardiaque en bpm
- **cadence** (INTEGER): Cadence de course ou de vélo
- **velocity_smooth** (DOUBLE PRECISION): Vitesse lissée en m/s
- **temp** (INTEGER): Température en Celsius
- **power** (INTEGER): Puissance en watts
- **grade_smooth** (DOUBLE PRECISION): Pente lissée en pourcentage

## Étapes de Migration

### Étape 1: Exécuter la Migration SQL

Cette étape ajoute les nouvelles colonnes à votre table `streams` existante:

```bash
cd eye_sight_backend
python migrations/add_stream_columns.py
```

**Ce que fait ce script:**
- Vérifie que la table `streams` existe
- Ajoute les 6 nouvelles colonnes (uniquement si elles n'existent pas déjà)
- Les nouvelles colonnes acceptent NULL (vos données existantes ne sont pas affectées)
- Affiche le schéma final de la table

**Sortie attendue:**
```
🔄 Starting migration: Adding new stream columns...
📋 Colonnes existantes: {'activity_id', 'lat', 'lon', 'altitude', 'distance_m', 'time_s'}
  ➕ Ajout de la colonne 'heartrate' (INTEGER)...
  ✅ Colonne 'heartrate' ajoutée
  [...]
✅ Migration terminée!
   6 nouvelle(s) colonne(s) ajoutée(s)
```

### Étape 2: Récupérer les Données pour les Activités Existantes (Backfill)

Cette étape récupère les nouvelles données de streams depuis Strava pour toutes vos activités existantes:

```bash
cd eye_sight_backend
python scripts/backfill_streams.py
```

**Options disponibles:**

```bash
# Traiter toutes les activités
python scripts/backfill_streams.py

# Limiter à 10 activités pour tester
python scripts/backfill_streams.py --max 10

# Reprendre depuis une activité spécifique (utile si interruption)
python scripts/backfill_streams.py --start-from 123456789

# Ajuster la limite de rate limit (par défaut: 590 appels/15min)
python scripts/backfill_streams.py --rate-limit 500
```

**Ce que fait ce script:**
- Récupère tous les IDs d'activités ayant des streams dans la DB
- Pour chaque activité, appelle l'API Strava pour récupérer les nouveaux streams
- Met à jour les lignes existantes avec les nouvelles données
- Respecte les limites de rate limit Strava (pause automatique après 590 appels)
- Commit après chaque activité (reprise facile en cas d'erreur)

**Sortie attendue:**
```
🚀 Démarrage du backfill des streams...

🔑 Authentification Strava...
📋 Récupération des activités ayant des streams...
   ✅ 150 activités trouvées

[1/150] Activité 12345678...
Stream de l'activité 12345678 récupéré ✅
  ✅ 1523 lignes mises à jour
[2/150] Activité 12345679...
[...]

✅ Backfill terminé!
   📊 Total: 250000 lignes mises à jour
   🔄 150 activités traitées
   📞 150 appels API effectués
```

### Étape 3: Les Futurs Fetch Incluront Automatiquement les Nouvelles Données

À partir de maintenant, toutes les nouvelles activités récupérées incluront automatiquement les 6 nouveaux champs de streams:

```python
# Votre code existant fonctionne sans modification
from strava.fetch_strava import fetch_strava_data, fetch_stream
from strava.store_data import store_df_streams_in_postgresql_optimized

# Récupérer les nouvelles activités
activities_df = fetch_strava_data()
# ...

# Récupérer les streams (inclut maintenant les 6 nouveaux champs)
df_stream = fetch_stream(activity_id, header)

# Stocker dans la DB (gère automatiquement les nouvelles colonnes)
store_df_streams_in_postgresql_optimized(df_stream, ...)
```

## Vérification

Pour vérifier que tout a fonctionné correctement:

```sql
-- Vérifier le schéma de la table
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'streams'
ORDER BY ordinal_position;

-- Vérifier les données (exemple avec heartrate)
SELECT
    COUNT(*) as total_lignes,
    COUNT(heartrate) as lignes_avec_heartrate,
    ROUND(100.0 * COUNT(heartrate) / COUNT(*), 2) as pourcentage
FROM streams;

-- Voir un exemple de données
SELECT *
FROM streams
WHERE heartrate IS NOT NULL
LIMIT 10;
```

## Notes Importantes

1. **Backup**: Il est recommandé de faire un backup de votre base de données avant la migration
2. **Rate Limits Strava**: Le backfill respecte les limites de 600 requêtes/15min de Strava
3. **Données manquantes**: Certains champs peuvent être NULL si Strava ne les fournit pas (ex: pas de capteur de puissance)
4. **Interruption**: Si le backfill est interrompu, utilisez `--start-from` pour reprendre
5. **Temps d'exécution**: Pour 150 activités, comptez environ 10-15 minutes

## Rollback (si nécessaire)

Si vous souhaitez annuler la migration:

```sql
-- Supprimer les nouvelles colonnes
ALTER TABLE streams DROP COLUMN IF EXISTS heartrate;
ALTER TABLE streams DROP COLUMN IF EXISTS cadence;
ALTER TABLE streams DROP COLUMN IF EXISTS velocity_smooth;
ALTER TABLE streams DROP COLUMN IF EXISTS temp;
ALTER TABLE streams DROP COLUMN IF EXISTS power;
ALTER TABLE streams DROP COLUMN IF EXISTS grade_smooth;
```

## Troubleshooting

### La migration échoue avec "table streams n'existe pas"
➡️ Vous devez d'abord créer des streams avec votre système actuel

### Le backfill est très lent
➡️ C'est normal, Strava limite à 600 requêtes/15min. Le script fait des pauses automatiques.

### Erreur "Rate limit exceeded"
➡️ Attendez 15 minutes et relancez avec `--start-from` pour reprendre

### Certaines colonnes restent NULL
➡️ Normal si l'activité n'a pas ces données (ex: pas de capteur de puissance ou cardio)
