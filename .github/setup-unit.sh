rm -f src/test/resources/ssh/id_rsa*

ssh-keygen -t rsa -b 4096 -m PEM -f src/test/resources/ssh/id_rsa -q -N ""

docker compose -f docker-compose-ci.yml up -d
sleep 3

docker exec plugin-fs-sftp-1 mkdir -p /home/foo/.ssh

PUBLIC_KEY=$(cat src/test/resources/ssh/id_rsa.pub)
docker exec plugin-fs-sftp-1 sh -c "echo '$PUBLIC_KEY' > /home/foo/.ssh/authorized_keys"

docker exec plugin-fs-sftp-1 chmod 700 /home/foo/.ssh
docker exec plugin-fs-sftp-1 chmod 600 /home/foo/.ssh/authorized_keys
