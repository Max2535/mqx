# The short demo at the top of the README: the two things mqx is built for, then the TUI.
# The full tour is docs/demo.tape.
#
# Record with: vhs docs/hero.tape  (needs the docker-compose stack and mqx on PATH)
# Seed it first:
#   export MQX_CONFIG=$PWD/deploy/mqx-config.yaml MQX_RABBIT_USER=guest MQX_RABBIT_PASS=guest
#   mqx topic create orders --partitions 3 -y && mqx topic create payments --partitions 2 -y
#   printf 'o-1001\t{"order_id":"o-1001","total":42.5}\n' | mqx publish orders --lines --key-separator $'\t' -y
#   # three consumers in group "fraud" on a two-partition topic, so one is idle
#   for i in 1 2 3; do docker exec -d mqx-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
#     --bootstrap-server localhost:9092 --topic payments --group fraud --consumer-property client.id=fraud; done
#   mqx exchange declare orders.x --type topic -c local-rabbit -y
#   mqx exchange declare audit.x --type fanout -c local-rabbit -y
#   mqx queue create orders.eu -c local-rabbit -y && mqx queue create orders.all --type quorum -c local-rabbit -y
#   mqx queue create audit.q -c local-rabbit -y
#   mqx bind --source orders.x --destination orders.eu --key 'order.*.eu' -c local-rabbit -y
#   mqx bind --source orders.x --destination orders.all --key 'order.#' -c local-rabbit -y
#   mqx bind --source orders.x --destination audit.x --destination-type exchange --key '#' -c local-rabbit -y
#   mqx bind --source audit.x --destination audit.q -c local-rabbit -y
Output docs/hero.gif
Set Shell bash
Set FontSize 14
Set Width 1400
Set Height 640
Set Theme "Catppuccin Mocha"
Set TypingSpeed 35ms
Set Padding 20

Hide
Type "export MQX_CONFIG=$PWD/deploy/mqx-config.yaml MQX_RABBIT_USER=guest MQX_RABBIT_PASS=guest PS1='$ ' && clear"
Enter
Show

# Kafka: why is this group misbehaving?
Type "mqx group diagnose fraud --interval 2s" Enter Sleep 5.5s
Sleep 3s

# RabbitMQ: where does this routing key go, without publishing?
Type "clear && mqx route orders.x --key order.created.eu -c local-rabbit" Enter Sleep 4s

# Safety: read-only contexts refuse mutations
Type "mqx topic delete orders -c local-kafka-ro -y" Enter Sleep 3s

# The same in a TUI: jump to the group with the command palette and open it
Type "clear && mqx" Enter Sleep 2.5s
Tab Sleep 1.2s
Tab Sleep 1.2s
Type ":" Sleep 0.5s
Type "fraud" Sleep 1.2s
Enter Sleep 1.5s
Enter Sleep 3.5s
Type "q"
Sleep 0.5s
