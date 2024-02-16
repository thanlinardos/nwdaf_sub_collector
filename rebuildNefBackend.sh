cd ./NEF_emulator &&
make build &&
docker stop nef_emulator-backend-1 &&
docker rm nef_emulator-backend-1 &&
make upd &&
docker logs nef_emulator-backend-1 --follow