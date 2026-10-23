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
A ser implementada.

## Como executar
Requisitos: Python 3 e `pip install requests` nas instâncias dos peers.

1. Edite o `constMP.py` com o IP público da instância do servidor e o número de peers (`N`).
2. Siga a ordem descrita na seção "Como executar" de [COMPARACAO_RPC.md](rpc_communication/COMPARACAO_RPC.md).
