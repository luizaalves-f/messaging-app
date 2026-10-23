# Sockets vs. RPC no MPComm

As interações cliente-servidor do MPComm foram reimplementadas com RPC em `rpc_communication/`, usando `xmlrpc`, que faz parte da biblioteca padrão do Python. Os arquivos originais do professor continuam em `MPComm/` para permitir a comparação.

## O que mudou

- **Registro de peers:** com sockets, o peer montava o dicionário, serializava com pickle e enviava. Com RPC, ele chama `groupMngr.register(ip, porta)` e a biblioteca faz a serialização.
- **Listagem de peers:** com sockets, o cliente enviava `{"op": "list"}` e recebia a lista com `recv(2048)`. Com RPC, é só `groupMngr.list()`.
- **Encerramento do GroupMngr:** com sockets, o comparisonServer enviava `{"op": "stop"}`. Com RPC, ele chama `groupMngr.stop()`.
- **Sinal de início:** com sockets, o comparisonServer abria uma conexão na porta 5679 e enviava a tupla `(id, nMsgs)`. Com RPC, ele chama `peer.start(id, nMsgs)` na mesma porta, que continua sendo TCP, porque o XML-RPC roda sobre HTTP.
- **Envio do log:** com sockets, o peer abria uma conexão e o comparisonServer lia com `recv(32768)`. Com RPC, o peer chama `comparisonServer.submitLog(log)`.
- No servidor, o `if/elif` sobre `req["op"]` foi substituído por `register_function`, que associa cada nome de operação a uma função Python.
- O peer passou a rodar um servidor RPC numa thread separada para receber o `start()`. A chamada coloca `(id, nMsgs)` numa fila (`queue.Queue`), e a thread principal espera nessa fila, como antes esperava no `accept()`. O comparisonServer faz o mesmo com os logs.

## O que permanece igual

Toda a comunicação entre peers (handshakes, mensagens de dados e avisos de fim) continua em UDP. Ela não é cliente-servidor, e o experimento mede justamente a desordem que o UDP permite. Com RPC, que é síncrono e roda sobre TCP, o experimento passaria a medir outra coisa.

## Vantagens do RPC

1. **Mensagens sempre completas.** TCP não separa mensagens, e a versão com sockets supõe que cada uma chega inteira num único `recv` de tamanho fixo. Um log maior que 32768 bytes seria cortado e o `pickle.loads` falharia. O HTTP informa o tamanho da mensagem (`Content-Length`), então isso não acontece.
2. **Segurança.** Fazer `pickle.loads` sobre dados recebidos numa porta pública permite que qualquer pessoa execute código no servidor. O XML-RPC só reconstrói tipos simples. A comunicação UDP entre peers continua usando `pickle`.
3. **Erros chegam ao cliente.** Com sockets, uma operação desconhecida é ignorada e o cliente pode ficar esperando para sempre (o `# fix` do professor). Com RPC, o cliente recebe uma exceção `Fault` automaticamente.
4. **Código mais simples.** As operações viram funções com parâmetros, sem `dumps`/`loads`/`send`/`recv` espalhados.
5. **Independente de linguagem.** Qualquer linguagem com cliente XML-RPC consegue falar com os servidores; com `pickle`, só Python.

## Desvantagens do RPC

1. **Mais overhead.** Cada chamada leva cabeçalhos HTTP e XML, maiores que o `pickle`, e abre uma conexão TCP nova. Aqui o impacto é pequeno, porque são poucas chamadas.
2. **Tipos limitados.** Tuplas viram listas, `None` e inteiros acima de 32 bits precisam de configuração, e objetos arbitrários não passam.
3. **Parece local, mas não é.** `groupMngr.list()` pode demorar, falhar ou travar, e o `ServerProxy` não tem timeout por padrão.
4. **Mais estrutura nos peers.** Cada peer precisa de um servidor RPC em outra thread e de uma fila. O `stop()` também não pode chamar `server.shutdown()` direto, porque daria deadlock: o `shutdown()` espera o `serve_forever()` terminar, que espera o próprio `stop()` retornar.
5. **Não serve para o multicast entre peers,** já que o RPC é um-para-um e síncrono.
6. **Uma requisição por vez.** Assim como a versão com sockets, o `SimpleXMLRPCServer` não atende chamadas em paralelo.

## Como executar

Mesma ordem e mesmas regras de Security Group da versão original:

1. Na instância do servidor: `python3 GroupMngrRPC.py`
2. Em cada peer: `python3 peerCommunicatorRPC.py`
3. No servidor, em outro terminal: `python3 comparisonServerRPC.py`

## Observações

- A lógica de comparação foi mantida igual à original, incluindo os limites `range(0, N_MSGS-1)` e `range(1, N-1)`, que com `N = 2` não comparam nenhum par de logs.
- O `unregister` continua sem implementação nas duas versões.
