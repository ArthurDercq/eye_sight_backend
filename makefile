

install_requirements:
	@pip install -r requirements.txt

run:
	@uvicorn main:app --reload --port 8000
