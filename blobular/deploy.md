
# Steps to deploy on AWS

Some guides that I found useful:
- [deploying fastapi](https://www.slingacademy.com/article/deploying-fastapi-on-ubuntu-with-nginx-and-lets-encrypt/)
- [how to serve flask apps](https://www.digitalocean.com/community/tutorials/how-to-serve-flask-applications-with-gunicorn-and-nginx-on-ubuntu-18-04)


Set up an EC2 instance (t2.large with Ubuntu running on it).
Install stuff:

```sh
sudo apt update
sudo apt install python3-pip python3-dev build-essential libssl-dev libffi-dev python3-setuptools

# nginx
sudo apt install nginx certbot python3-certbot-nginx
sudo ufw allow 'Nginx Full'
```

Clone the repo

```sh
git clone git@github.com:EdAyers/sss.git
cd sss
python -m venv .venv
source .venv/bin/activate

pip install -e ./dxd
pip install -e ./blobular

# [todo] there might be some other pip deps not in pyproject

cd blobular
```

Then add the `.env` file.

Splat this in `/etc/systemd/system/blobular.service`

```ini
[Unit]
Description=Gunicorn Daemon for blobular
After=network.target

[Service]
User=ubuntu
Group=www-data
WorkingDirectory=/home/ubuntu/sss/blobular
ExecStart=/home/ubuntu/sss/.venv/bin/gunicorn -c gunicorn_conf.py blobular.api.app:app

[Install]
WantedBy=multi-user.target
```

Splat this in  `/etc/nginx/sites-available/blobular`

```nginx
server {
        client_max_body_size 4G;
        server_name $YOUR_DOMAIN; # ← put your domain name here.

        location / {
                proxy_pass             http://127.0.0.1:8001;
                proxy_read_timeout     60;
                proxy_connect_timeout  60;
                proxy_redirect         off;

                # Allow the use of websockets
                proxy_http_version 1.1;
                proxy_set_header Upgrade $http_upgrade;
                proxy_set_header Connection 'upgrade';
                proxy_set_header Host $host;
                proxy_cache_bypass $http_upgrade;
        }
}
```

do

```sh
sudo ln -s /etc/nginx/sites-available/blobular /etc/nginx/sites-enabled

sudo systemctl enable --now blobular
sudo systemctl enable --now nginx
```

If you want logs you can do `tail logs/error_log`.

Connect it to your domain with elastic IPs and A-records etc.

Then add TLS:

```sh
sudo certbot --nginx -d $YOUR_DOMAIN
sudo systemctl restart nginx
```

That should be it!