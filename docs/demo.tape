# Record with: vhs docs/demo.tape  (needs the docker-compose stack and mqx on PATH)
# Seed it first:
#   mqx topic create orders --partitions 3 -y && mqx topic create payments --partitions 2 -y
#   printf 'o-1001\t{"order_id":"o-1001","total":42.5}\n' | mqx publish orders --lines --key-separator $'\t' -H source=web -y
#   mqx exchange declare orders.x --type topic -c local-rabbit -y
#   mqx queue create orders.eu -c local-rabbit -y && mqx queue create orders.all --type quorum -c local-rabbit -y
#   mqx bind --source orders.x --destination orders.eu --key 'order.*.eu' -c local-rabbit -y
#   mqx bind --source orders.x --destination orders.all --key 'order.#' -c local-rabbit -y
Output docs/demo.gif
Set Shell bash
Set FontSize 15
Set Width 1200
Set Height 720
Set Theme "Catppuccin Mocha"
Set TypingSpeed 40ms

Hide
Type "export MQX_CONFIG=$PWD/deploy/mqx-config.yaml MQX_RABBIT_USER=guest MQX_RABBIT_PASS=guest PS1='$ ' && clear"
Enter
Show

# CLI: contexts, topics, peek, route dry-run, read-only refusal
Type "mqx ctx list" Enter Sleep 1.5s
Type "mqx topics" Enter Sleep 1.5s
Type "mqx peek orders -n 1" Enter Sleep 2s
Type "mqx route orders.x --key order.created.eu -c local-rabbit" Enter Sleep 2.5s
Type "mqx topic delete orders -c local-kafka-ro -y" Enter Sleep 2.5s

# TUI: command palette jumps to a topic and runs an action
Type "clear && mqx" Enter Sleep 2s
Type ":" Sleep 0.6s
Type "pay" Sleep 1.2s
Enter Sleep 0.8s
Type ":" Sleep 0.4s
Type "pub" Sleep 1.2s
Enter Sleep 1s

# Publish with JSON headers, then bring the submission back with ctrl+r
Type "p-2" Tab Tab
Type '{"source":"tui"}' Tab Tab
Type '{"payment_id":"p-2","status":"captured"}' Sleep 0.8s
Ctrl+S Sleep 1.5s
Type "y" Sleep 1.5s
Type "p" Sleep 0.8s
Ctrl+R Sleep 2.5s
Escape Sleep 0.5s

# Contextual hints while creating a topic
Type "n" Sleep 0.6s
Type "billing.events_v1" Tab
Type "6" Tab
Type "3" Sleep 3.5s
Escape Sleep 0.5s

# Message viewer
Type "m" Sleep 2.5s
Escape Sleep 0.5s
Type "q"
Sleep 0.5s
