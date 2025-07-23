import com.google.gson.Gson;
import java.io.*;
import java.net.*;
import java.util.*;

/*
 * Projeto SD: ServidorS — Key-Value Store Distribuido
 * Funcionalidades conforme SECAO 5 do enunciado.
 */
public class Servidor {
    private static final Gson gson = new Gson();

    // HashMap principal (key -> valor+timestamp), protegido manualmente (5a, 5c)
    private static Map<String, DadoKV> banco = new HashMap<>();

    // True se este servidor e o lider (definido no teclado, 5a)
    private static boolean souLider = false;

    // Porta deste servidor (capturada no teclado, 5a)
    private static int porta;

    // IP e porta do lider (capturados no teclado, 5a)
    private static String ipLider;
    private static int portaLider;

    // Seguidores do lider (usado para replicacao, 5c2, APENAS NO LIDER)
    private static List<InetSocketAddress> servidoresSeguidores = new ArrayList<>();

    // Gets pendentes para suporte a WAIT_FOR_RESPONSE (5f)
    private static Map<String, List<Mensagem>> getsPendentes = new HashMap<>();

    /*
     * Estrutura auxiliar: value + timestamp por chave
     */
    private static class DadoKV {
        String valor;
        long timestamp;
        DadoKV(String valor, long timestamp) {
            this.valor = valor;
            this.timestamp = timestamp;
        }
    }

public static void main(String[] args) {
    Scanner scanner = new Scanner(System.in);

    // Pergunta uma única vez se os servidores estão na mesma máquina
    System.out.print("Os servidores estao na mesma maquina (127.0.0.1)? (s/n): ");
    boolean mesmaMaquina = scanner.nextLine().trim().equalsIgnoreCase("s");
    String ipPadrao = mesmaMaquina ? "127.0.0.1" : null;

    // Agora pergunta IP e porta deste servidor
    String ip;
    if (mesmaMaquina) {
        ip = ipPadrao;
        System.out.print("Digite a porta deste servidor (ex: 10097): ");
    } else {
        System.out.print("Digite o IP deste servidor (ex: 127.0.0.1): ");
        ip = scanner.nextLine().trim();
        System.out.print("Digite a porta deste servidor (ex: 10097): ");
    }
    porta = Integer.parseInt(scanner.nextLine().trim());

    // Define se é líder ou não
    System.out.print("Este servidor eh o lider? (s/n): ");
    souLider = scanner.nextLine().trim().equalsIgnoreCase("s");

    if (souLider) {
        ipLider = ip;
        portaLider = porta;
        for (int i = 1; i <= 2; i++) {
            String ipSeguidor;
            int portaSeguidor;
            if (mesmaMaquina) {
                ipSeguidor = ipPadrao;
                System.out.print("Digite a porta do servidor seguidor " + i + ": ");
                portaSeguidor = Integer.parseInt(scanner.nextLine().trim());
            } else {
                System.out.print("Digite o IP do servidor seguidor " + i + ": ");
                ipSeguidor = scanner.nextLine().trim();
                System.out.print("Digite a porta do servidor seguidor " + i + ": ");
                portaSeguidor = Integer.parseInt(scanner.nextLine().trim());
            }
            servidoresSeguidores.add(new InetSocketAddress(ipSeguidor, portaSeguidor));
        }
    } else {
        if (mesmaMaquina) {
            ipLider = ipPadrao;
            System.out.print("Digite a porta do lider: ");
            portaLider = Integer.parseInt(scanner.nextLine().trim());
        } else {
            System.out.print("Digite o IP do lider: ");
            ipLider = scanner.nextLine().trim();
            System.out.print("Digite a porta do lider: ");
            portaLider = Integer.parseInt(scanner.nextLine().trim());
        }
    }

    // Aceita conexoes simultaneas (thread por cliente) — 5b
    try (ServerSocket serverSocket = new ServerSocket(porta)) {
        System.out.println("Servidor escutando na porta " + porta + "...");
        while (true) {
            Socket clienteSocket = serverSocket.accept();
            new Thread(new ClienteHandler(clienteSocket)).start();
        }
    } catch (IOException e) {
        System.err.println("Erro ao iniciar servidor: " + e.getMessage());
    }
}

    /*
     * Classe de handler para cada conexao, permite concorrencia (thread)
     * Responsavel pelas operacoes 5c (PUT), 5d (REPLICATION), 5f (GET)
     */
    private static class ClienteHandler implements Runnable {
        private Socket socket;
        public ClienteHandler(Socket socket) {
            this.socket = socket;
        }

        public void run() {
            // Obtem IP e porta do cliente para prints exatos do enunciado
            String ipCliente = socket.getInetAddress().getHostAddress();
            int portaCliente = socket.getPort();

            try (
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
            ) {
                String jsonRecebido = in.readLine();
                Mensagem msg = gson.fromJson(jsonRecebido, Mensagem.class);

                // Switch para tratar PUT, GET, REPLICATION (5c, 5f, 5d)
                switch (msg.getTipo()) {
                    case "PUT":
                        tratarPUT(msg, out, ipCliente, portaCliente);
                        break;
                    case "GET":
                        tratarGET(msg, out, ipCliente, portaCliente);
                        break;
                    case "REPLICATION":
                        tratarREPLICATION(msg, out);
                        break;
                    default:
                        System.out.println("Tipo de mensagem nao reconhecido: " + msg.getTipo());
                }
            } catch (IOException e) {
                System.err.println("Erro ao tratar cliente: " + e.getMessage());
            } finally {
                try {
                    socket.close();
                } catch (IOException e) {}
            }
        }

        /*
         * SECAO 5c: PUT
         * - Se nao for lider, encaminha PUT para lider (TCP, 5c)
         * - Se for lider:
         *   1. Atualiza tabela local (banco), value+timestamp (protegido com synchronized)
         *   2. Replica nos seguidores (REPLICATION, TCP, 5c2, 5d)
         *   3. Quando recebe todos os REPLICATION_OK, envia PUT_OK ao cliente (5e)
         */
        private void tratarPUT(Mensagem msg, PrintWriter out, String ipCliente, int portaCliente) {
            if (souLider) {
                // Print exato do enunciado — lider
                System.out.println("Cliente " + ipCliente + ":" + portaCliente +
                        " PUT key:" + msg.getKey() + " value:" + msg.getValue() + ".");

                // Gera timestamp e atualiza hash local (protegido para concorrencia)
                long novoTimestamp = System.currentTimeMillis();
                synchronized (banco) {
                    banco.put(msg.getKey(), new DadoKV(msg.getValue(), novoTimestamp));
                }

                // Replicacao para seguidores via TCP (5c2)
                Mensagem replicacao = new Mensagem("REPLICATION", msg.getKey(), msg.getValue(), novoTimestamp, null, 0);
                int acks = 0;
                for (InetSocketAddress seguidor : servidoresSeguidores) {
                    System.out.println("Enviando REPLICATION para " + seguidor);

                    // Simula delay de replicacao, para testar WAIT_FOR_RESPONSE no GET dos seguidores
                    try {
                        System.out.println("Simulando delay de replicacao para o seguidor " + seguidor + " (10 segundos)...");
                        Thread.sleep(10000); // 10 segundos
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }

                    if (enviarReplicacao(seguidor, replicacao)) {
                        acks++;
                        System.out.println("Recebido REPLICATION_OK de " + seguidor);
                    }
                }
                if (acks == servidoresSeguidores.size()) {
                    // Envia PUT_OK apos replicacao (5e)
                    Mensagem resposta = new Mensagem("PUT_OK", msg.getKey(), msg.getValue(), novoTimestamp, null, 0);
                    out.println(gson.toJson(resposta));
                    System.out.println("Enviando PUT_OK ao Cliente " + ipCliente + ":" + portaCliente +
                            " da key:" + msg.getKey() + " ts:" + novoTimestamp + ".");
                    notificarPendentes(msg.getKey(), msg.getValue(), novoTimestamp);
                } else {
                    System.out.println("Erro na replicacao. PUT nao confirmado.");
                }
            } else {
                // Print para seguidor (nao-lider)
                System.out.println("Encaminhando PUT key:" + msg.getKey() + " value:" + msg.getValue());
                // Encaminha para o lider via TCP
                try (
                    Socket socketLider = new Socket(ipLider, portaLider);
                    PrintWriter outLider = new PrintWriter(socketLider.getOutputStream(), true);
                    BufferedReader inLider = new BufferedReader(new InputStreamReader(socketLider.getInputStream()))
                ) {
                    outLider.println(gson.toJson(msg));
                    String respostaLider = inLider.readLine();
                    out.println(respostaLider); // Responde ao cliente
                } catch (IOException e) {
                    System.out.println("Falha ao encaminhar PUT ao lider: " + e.getMessage());
                    out.println("{\"tipo\":\"PUT_FAIL\"}");
                }
            }
        }

        /*
         * SECAO 5f: GET
         * - Responde GET seguindo politica de timestamps
         *   1. Se nao existe, retorna NULL
         *   2. Se timestamp local >= do cliente, retorna valor/timestamp
         *   3. Se timestamp local < do cliente, retorna WAIT_FOR_RESPONSE, corta conexao e coloca cliente em pendentes
         */
        private void tratarGET(Mensagem msg, PrintWriter out, String ipCliente, int portaCliente) {
            String key = msg.getKey();
            long tsCliente = msg.getTimestamp();

            DadoKV dado;
            synchronized (banco) {
                dado = banco.get(key);
            }

            if (dado == null) {
                if (tsCliente == 0) {
                    // Nunca viu a chave
                    out.println(gson.toJson(new Mensagem("GET_OK", key, "NULL", 0, null, 0)));
                } else {
                    // Cliente já viu valor, mas servidor ainda não replicou
                    // Responde WAIT_FOR_RESPONSE imediatamente
                    synchronized (getsPendentes) {
                        getsPendentes.computeIfAbsent(key, k -> new ArrayList<>()).add(
                            new Mensagem(msg.getTipo(), key, null, tsCliente, msg.getIpCliente(), msg.getPortaCliente())
                        );
                    }
                    out.println(gson.toJson(new Mensagem("WAIT_FOR_RESPONSE", key, null, 0, null, 0)));
                }
                return;
            }

            if (dado.timestamp >= tsCliente) {
                // Servidor já tem valor igual ou mais novo, responde GET_OK
                out.println(gson.toJson(new Mensagem("GET_OK", key, dado.valor, dado.timestamp, null, 0)));
            } else {
                // Valor local antigo, responde WAIT_FOR_RESPONSE imediatamente
                synchronized (getsPendentes) {
                    getsPendentes.computeIfAbsent(key, k -> new ArrayList<>()).add(
                        new Mensagem(msg.getTipo(), key, null, tsCliente, msg.getIpCliente(), msg.getPortaCliente())
                    );
                }
                out.println(gson.toJson(new Mensagem("WAIT_FOR_RESPONSE", key, null, dado.timestamp, null, 0)));
            }
        }

        /*
         * SECAO 5d: REPLICATION — replica valor recebido do lider (TCP)
         * Atualiza hash local, responde com REPLICATION_OK
         */
        private void tratarREPLICATION(Mensagem msg, PrintWriter out) {
            synchronized (banco) {
                banco.put(msg.getKey(), new DadoKV(msg.getValue(), msg.getTimestamp()));
            }
            System.out.println("REPLICATION key:" + msg.getKey() + " value:" +
                    msg.getValue() + " ts:" + msg.getTimestamp() + ".");
            Mensagem ack = new Mensagem("REPLICATION_OK", msg.getKey(), null, msg.getTimestamp(), null, 0);
            out.println(gson.toJson(ack));
            notificarPendentes(msg.getKey(), msg.getValue(), msg.getTimestamp());
        }

        /*
         * Envia mensagem REPLICATION para seguidor via TCP e espera ACK (5c2)
         */
        private boolean enviarReplicacao(InetSocketAddress destino, Mensagem replicacao) {
            try (Socket socket = new Socket(destino.getAddress(), destino.getPort());
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                out.println(gson.toJson(replicacao));
                String respostaJson = in.readLine();
                Mensagem resposta = gson.fromJson(respostaJson, Mensagem.class);

                return "REPLICATION_OK".equals(resposta.getTipo());
            } catch (IOException e) {
                System.out.println("Falha ao replicar para " + destino + ": " + e.getMessage());
                return false;
            }
        }

        /*
         * (GET assincrono) — envia GET_OK para clientes pendentes assim que possivel (5f)
         */
        private void notificarPendentes(String key, String value, long timestamp) {
            List<Mensagem> pendentes;
            synchronized (getsPendentes) {
                pendentes = getsPendentes.remove(key);
            }

            if (pendentes != null) {
                for (Mensagem pendente : pendentes) {
                    System.out.println("[DEBUG SERVIDOR] Tentando resposta assincrona para " +
                        pendente.getIpCliente() + ":" + pendente.getPortaCliente());
                    try (
                        Socket socket = new Socket(pendente.getIpCliente(), pendente.getPortaCliente());
                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
                    ) {
                        Mensagem resposta = new Mensagem("GET_OK", key, value, timestamp, null, 0);
                        out.println(gson.toJson(resposta));
                        System.out.println("Enviado GET_OK assincrono para " +
                                pendente.getIpCliente() + ":" + pendente.getPortaCliente() + " key:" + key);
                    } catch (IOException e) {
                        System.out.println("Erro ao enviar resposta assincrona para cliente: " + e.getMessage());
                    }
                }
            }
        }
    }
}
