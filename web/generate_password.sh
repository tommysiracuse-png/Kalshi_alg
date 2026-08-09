cd ~/Kalshi_alg/web

read -rsp "New UI password: " UI_PASSWORD
echo

UI_HASH=$(UI_PASSWORD="$UI_PASSWORD" node -e \
  'require("argon2").hash(process.env.UI_PASSWORD, {type: require("argon2").argon2id}).then(console.log)')

unset UI_PASSWORD
echo "$UI_HASH"
