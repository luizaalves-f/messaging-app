# messaging-app
Aplicação de troca de mensagens para o trabalho final de Sistemas Distribuídos.

## Parte 1
a) Substituir sockets por RPC nas interações cliente-servidor e comparar em termos de vantagens e desvantagens.

[MPComm](MPComm/) foi clonado do repositório original do professor Fábio, disponível em:
https://github.com/professorfabio/MPComm/tree/main
e foi mantido intacto em `MPComm/` para comparação.

A partir da implementação do professor, foi implementada a letra a) da parte 1 do Trabalho Final, trocando a comunicação por sockets por RPC em [rpc_communication/](rpc_communication/).
A explicação das vantagens e desvantagens está documentada em [rpc_communication/COMPARACAO_RPC.md](rpc_communication/COMPARACAO_RPC.md).

b) Implementar tratamento para perda de mensagens e ordenação FIFO na comunicação multicast (mantendo o uso de sockets).

Implementada em [reliable_multicast/](reliable_multicast/), a partir do `MPComm/` original (sockets UDP/TCP, sem RPC). A lógica fica em [reliableMulticast.py](reliable_multicast/reliableMulticast.py), na classe `ReliableFifoMulticast`:

- **Perda de mensagens:** cada `DATA` enviado a um peer é guardado até chegar o `ACK` desse peer, e é reenviado a cada `RETRANSMIT_TIMEOUT` (0,2 s) enquanto não chegar. O receptor responde `ACK` a toda mensagem recebida, inclusive duplicadas (o `ACK` também pode se perder).
- **Ordem FIFO:** cada remetente numera suas mensagens (0, 1, 2, ...). O receptor guarda, por remetente, o próximo número esperado: mensagens adiantadas ficam num buffer até as anteriores chegarem, e duplicadas são descartadas. A entrega ao log só acontece em ordem.
- **Handshake e fim:** o `READY` virou `HS`/`HSACK` com retransmissão, e o aviso de fim virou uma mensagem `DATA` normal (com carga `None`), então também é confiável. O peer só termina quando recebeu o fim de todos e todas as suas mensagens foram confirmadas. Depois disso ele continua respondendo `ACK` por `LINGER_TIME` (2 s), para o caso de o último `ACK` ter se perdido.
- **Rodadas:** todo pacote leva o número da rodada, para que pacotes atrasados de uma execução anterior não contaminem a seguinte.
- **comparisonServer:** além da métrica original, verifica em cada log se as mensagens de cada remetente são exatamente `0..nMsgs-1` em ordem (sem perda, duplicação ou inversão) e passou a ler o log em loop, sem o limite de 32768 bytes do `recv`.
- **Simulação de perda:** `LOSS_RATE` em `constMP.py` descarta essa fração dos pacotes recebidos (por exemplo `0.3`), para demonstrar o mecanismo mesmo na mesma região da nuvem.

A métrica original de mensagens fora de ordem continua podendo ser maior que zero: FIFO só garante a ordem de cada remetente, não a mesma ordem entre todos os peers (isso exigiria ordenação total).

Para executar, use a mesma ordem da seção abaixo, com `GroupMngr.py`, `peerCommunicator.py` e `comparisonServer.py` de `reliable_multicast/`.

## Como executar
Requisitos: Python 3 e `pip install requests` nas instâncias dos peers.

1. Edite o `constMP.py` com o IP público da instância do servidor e o número de peers (`N`).
2. Siga a ordem descrita na seção "Como executar" de [COMPARACAO_RPC.md](rpc_communication/COMPARACAO_RPC.md).
