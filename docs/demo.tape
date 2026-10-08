# Record with: vhs docs/demo.tape  (needs the docker-compose stack and mqx on PATH)
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

Type "mqx ctx list" Enter Sleep 1.5s
Type "mqx topics" Enter Sleep 1.5s
Type "mqx peek orders -n 2" Enter Sleep 2.5s
Type "mqx route orders.x --key order.created -c local-rabbit" Enter Sleep 2.5s
Type "mqx topic delete orders -c local-kafka-ro -y" Enter Sleep 2.5s
Type "clear && mqx" Enter Sleep 2.5s
Down Sleep 0.6s
Down Sleep 0.6s

Type "m" Sleep 2.5s
Down Sleep 1.5s
Escape Sleep 0.5s
Escape Sleep 0.5s
Tab Sleep 1.5s
Tab Sleep 1.5s
Type "c" Sleep 1.5s
Down Sleep 0.5s Down Sleep 0.5s
Enter Sleep 2.5s
Tab Sleep 1.5s
Type "q"
Sleep 0.5s
