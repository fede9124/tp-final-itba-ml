 # DOCKER INSTALLATION
 
 # Set up repository
sudo apt-get update

sudo apt-get install \
   ca-certificates \
   curl \
   gnupg \
   lsb-release


sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg


echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null


# Docker Engine Installation

#Última versión
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Por si quiero usar una versión en particular
#VERSION_STRING=5:20.10.13~3-0~ubuntu-jammy
#sudo apt-get install docker-ce=$VERSION_STRING docker-ce-cli=$VERSION_STRING containerd.io docker-compose-plugin


# Corroborar instalación correcta
sudo docker run hello-world


