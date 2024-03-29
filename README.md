# CargoSync
Tool based on Containerd for creating and applying rsync-based delta updates for container images.

## Installation
Only dependencies are Containerd, rsync, and the Go programming language

The following were tested on Ubuntu 22.04 VMs.

Installing Containerd:
```bash
wget https://github.com/containerd/containerd/releases/download/v1.7.11/containerd-1.7.11-linux-amd64.tar.gz
sudo tar Cxzvf /usr/local containerd-1.7.11-linux-amd64.tar.gz
sudo mkdir -p /usr/local/lib/systemd/system # This path most probably already exists
sudo curl https://raw.githubusercontent.com/containerd/containerd/main/containerd.service -o /usr/local/lib/systemd/system/containerd.service
sudo systemctl daemon-reload
sudo systemctl enable --now containerd
```
Installing Go:
```bash
wget https://go.dev/dl/go1.21.6.linux-amd64.tar.gz
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.21.6.linux-amd64.tar.gz # you may need to run this as root
export PATH=$PATH:/usr/local/go/bin
go version
sudo su
export PATH=$PATH:/usr/local/go/bin
```

Installing rsync: 

If rsync is not already installed at your machines, follow the instructions [here](https://download.samba.org/pub/rsync/INSTALL) after downloading the source tar:
```bash
wget https://download.samba.org/pub/rsync/src-previews/rsync-3.3.0pre1.tar.gz
tar xvf rsync-3.3.0pre1.tar.gz
cd rsync-3.3.0pre1.tar.gz
```

## Usage

**Setup with 2 VMs (Server/Client)**:

*note: you should probably run the following as root*

Compile the go files on both machines
```bash
make
```
...or simply run 
```bash
go mod tidy
go build -o client client/client.go
go build -o server server/server.go
```

On the server machine: run the server executable and specify the address and port the service will listen for requests:
```bash
server/server 0.0.0.0:4000 # Listen to all interfaces on port 4000
```

On the client machine: First, we need the base image, if we haven't got one already. Then we can make a request to the server application to produce and send the delta diffs

Example: (The tensorflow target image below is over 1GB, if you want to try it with a smaller image you can use something like docker.io/library/zookeeper:{3.9.1, latest}, or anything else)
```bash
ctr image pull nvcr.io/nvidia/tensorflow:18.01-py3
client/client nvcr.io/nvidia/tensorflow:18.02-py3 10.182.0.5:4000 # Replace this with the IP and port address of the server application 
```
The newest image that is locally available in the client is automatically selected as the base image


Now the client will pull the rsync-based delta from the server machine and apply it to the existing image to produce the updated version.

## Acknowledgement
The project has received funding from the European Union’s Horizon Europe programme under Grant Agreement N°101135959.
