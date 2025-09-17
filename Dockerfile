# Utiliser une image Python légère
FROM python:3.12-slim

# Définir le dossier de travail
WORKDIR /eye_sight

# Copier uniquement le fichier requirements pour le cache
COPY requirements.txt .

# Installer les dépendances
RUN pip install --no-cache-dir -r requirements.txt

# Copier le reste du code
COPY . .

# Exposer le port FastAPI
EXPOSE 8000

# Commande pour lancer l'API
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
