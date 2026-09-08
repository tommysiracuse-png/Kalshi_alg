cd ~/Kalshi_alg/web
npm run build
systemctl --user restart kalshi-ui-api.service kalshi-ui-web.service
