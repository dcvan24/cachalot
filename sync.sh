rsync -avr -e "ssh -l cc" --include 'cfg.json' --exclude '*.pyc' --exclude '*.json'  --exclude '*.pdf' --exclude '.idea' --exclude '.DS_Store' --exclude '__pycache__' ../sim/ $1:~/sim
