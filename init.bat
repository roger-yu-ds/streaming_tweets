@REM Get Docker files
Invoke-WebRequest https://raw.githubusercontent.com/bde-uts/bde/main/bde_lab_8/Dockerfile -OutFile "./Dockerfile"
Invoke-WebRequest https://raw.githubusercontent.com/bde-uts/bde/main/bde_lab_8/docker-compose.yml -OutFile "./docker-compose.yml"

@REM Initialise pipenv
pipenv install numpy pandas ipykernel validators

@REM Initialise Docker
docker-compose up -d
