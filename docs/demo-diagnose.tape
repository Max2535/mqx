# Record with: vhs docs/demo-diagnose.tape
# Needs: `docker compose up -d kafka` running and mqx on PATH.
# Scenario: a consumer group "billing" read part of a topic and exited, so the group is
# empty but still has lag. `group diagnose` should report that finding.
Output docs/demo-diagnose.gif
Set Shell bash
Set FontSize 15
Set Width 1200
Set Height 720
Set Theme "Catppuccin Mocha"
Set TypingSpeed 40ms

Hide
Type "export MQX_CONFIG=$PWD/deploy/mqx-config.yaml PS1='$ ' && clear"
Enter
Type "docker compose up -d --wait kafka >/dev/null 2>&1"
Enter Sleep 20s
Type "k() { docker compose exec -T kafka /opt/kafka/bin/\"$@\"; }; B='--bootstrap-server localhost:9092'"
Enter
Type "k kafka-topics.sh $B --delete --topic billing-events >/dev/null 2>&1; k kafka-consumer-groups.sh $B --delete --group billing >/dev/null 2>&1"
Enter Sleep 3s
Type "k kafka-topics.sh $B --create --topic billing-events --partitions 3 >/dev/null 2>&1"
Enter Sleep 3s
Type "seq 1 30 | k kafka-console-producer.sh $B --topic billing-events >/dev/null 2>&1"
Enter Sleep 4s
Type "k kafka-console-consumer.sh $B --topic billing-events --group billing --from-beginning --max-messages 5 >/dev/null 2>&1"
Enter Sleep 10s
Type "clear"
Enter
Show

Type "# a consumer group stopped and messages are piling up. Why?" Enter Sleep 1.5s
Type "mqx group describe billing -c local-kafka" Enter Sleep 3s
Type "mqx group lag billing -c local-kafka" Enter Sleep 3s
Type "mqx group diagnose billing -c local-kafka" Enter Sleep 6s
