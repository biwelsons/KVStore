import com.google.gson.Gson;
import java.io.*;
import java.net.*;
import java.util.*;

/*
 * Projeto SD: ServidorS — Key-Value Store Distribuído
 * Funcionalidades implementadas conforme SEÇÃO 5 da documentação técnica.
 */
public class Servidor {
    private static final Gson gson = new Gson();

    // HashMap principal (key -> valor+timestamp), protegido manualmente (5a, 5c)
    private static Map<String, DadoKV> banco = new HashMap<>();

    // True se este servidor é o líder (definido no teclado, 5a)
    private static boolean souLider = false;

    // Porta deste servidor (capturada no teclado, 5a)
    private static int porta;

    // IP e porta do líder (capturados no teclado, 5a)
    private static String ipLider;
    private static int portaLider;

    // Seguidores do líder (usado para replicação, 5c2)
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

        // Inicialização manual de IP e porta do servidor (5a)
        System.out.print("Digite o IP deste servidor (ex: 127.0.0.1): ");
        String ip = scanner.nextLine().trim();
        System.out.print("Digite a porta deste servidor (ex: 10097): ");
        porta = Integer.parseInt(scanner.nextLine().trim());

        // Define se é líder ou não
        System.out.print("Este servidor eh o lider? (s/n): ");
        souLider = scanner.nextLine().trim().equalsIgnoreCase("s");

        if (souLider) {
            // Coleta lista de seguidores para replicação (5c2)
            ipLider = ip;
            portaLider = porta;
            for (int i = 1; i <= 2; i++) {
                System.out.print("Digite o IP do servidor seguidor " + i + ": ");
                String ipSeguidor = scanner.nextLine().trim();
                System.out.print("Digite a porta do servidor seguidor " + i + ": ");
                int portaSeguidor = Integer.parseInt(scanner.nextLine().trim());
                servidoresSeguidores.add(new InetSocketAddress(ipSeguidor, portaSeguidor));
            }
        } else {
            // Seguidor pergunta pelo líder (5a)
            System.out.print("Digite o IP do lider: ");
            ipLider = scanner.nextLine().trim();
            System.out.print("Digite a porta do lider: ");
            portaLider = Integer.parseInt(scanner.nextLine().trim());
        }

        // Aceita conexões simultâneas (thread por cliente) — 5b
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
     * Classe de handler para cada conexão, permite concorrência (thread)
     * Responsável pelas operações 5c (PUT), 5d (REPLICATION), 5f (GET)
     */
    private static class ClienteHandler implements Runnable {
        private Socket socket;
        public ClienteHandler(Socket socket) {
            this.socket = socket;
        }

        public void run() {
            // Obtém IP e porta do cliente para prints exatos do enunciado
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
         * SEÇÃO 5c: PUT
         * - Se não for líder, encaminha PUT para líder (TCP, 5c)
         * - Se for líder:
         *   1. Atualiza tabela local (banco), value+timestamp (protegido com synchronized)
         *   2. Replica nos seguidores (REPLICATION, TCP, 5c2, 5d)
         *   3. Quando recebe todos os REPLICATION_OK, envia PUT_OK ao cliente (5e)
         */
        private void tratarPUT(Mensagem msg, PrintWriter out, String ipCliente, int portaCliente) {
            if (souLider) {
                // Print exato do enunciado — líder
                System.out.println("Cliente " + ipCliente + ":" + portaCliente +
                        " PUT key:" + msg.getKey() + " value:" + msg.getValue() + ".");

                // Gera timestamp e atualiza hash local (protegido para concorrência)
                long novoTimestamp = System.currentTimeMillis();
                synchronized (banco) {
                    banco.put(msg.getKey(), new DadoKV(msg.getValue(), novoTimestamp));
                }

                // Replicação para seguidores via TCP (5c2)
                Mensagem replicacao = new Mensagem("REPLICATION", msg.getKey(), msg.getValue(), novoTimestamp, null, 0);
                int acks = 0;
                for (InetSocketAddress seguidor : servidoresSeguidores) {
                    System.out.println("Enviando REPLICATION para " + seguidor);
                    if (enviarReplicacao(seguidor, replicacao)) {
                        acks++;
                        System.out.println("Recebido REPLICATION_OK de " + seguidor);
                    }
                }
                if (acks == servidoresSeguidores.size()) {
                    // Envia PUT_OK após replicação (5e)
                    Mensagem resposta = new Mensagem("PUT_OK", msg.getKey(), msg.getValue(), novoTimestamp, null, 0);
                    out.println(gson.toJson(resposta));
                    System.out.println("Enviando PUT_OK ao Cliente " + ipCliente + ":" + portaCliente +
                            " da key:" + msg.getKey() + " ts:" + novoTimestamp + ".");
                    notificarPendentes(msg.getKey(), msg.getValue(), novoTimestamp);
                } else {
                    System.out.println("Erro na replicacao. PUT nao confirmado.");
                }
            } else {
                // Print
                System.out.println("Encaminhando PUT key:" + msg.getKey() + " value:" + msg.getValue());
                // Encaminha para o líder via TCP
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
         * SEÇÃO 5f: GET
         * - Responde GET seguindo política de timestamps
         *   1. Se não existe, retorna NULL
         *   2. Se timestamp local >= do cliente, retorna valor/timestamp
         *   3. Se timestamp local < do cliente, retorna WAIT_FOR_RESPONSE, corta conexão e coloca cliente em pendentes
         */
        private void tratarGET(Mensagem msg, PrintWriter out, String ipCliente, int portaCliente) {
            String key = msg.getKey();
            long tsCliente = msg.getTimestamp();

            DadoKV dado;
            synchronized (banco) {
                dado = banco.get(key);
            }

            if (dado == null) {
                // Print exato do enunciado para key ausente
                System.out.println("Cliente " + ipCliente + ":" + portaCliente +
                        " GET key:" + key + " ts:" + tsCliente +
                        ". Meu ts é NULL, portanto devolvendo NULL.");
                Mensagem resposta = new Mensagem("GET_OK", key, "NULL", 0, null, 0);
                out.println(gson.toJson(resposta));
                return;
            }

            if (dado.timestamp >= tsCliente) {
                // Print exato do enunciado para value atualizado
                System.out.println("Cliente " + ipCliente + ":" + portaCliente +
                        " GET key:" + key + " ts:" + tsCliente +
                        ". Meu ts é " + dado.timestamp + ", portanto devolvendo " + dado.valor + ".");
                Mensagem resposta = new Mensagem("GET_OK", key, dado.valor, dado.timestamp, null, 0);
                out.println(gson.toJson(resposta));
            } else {
                // Print exato do enunciado para WAIT_FOR_RESPONSE
                System.out.println("Cliente " + ipCliente + ":" + portaCliente +
                        " GET key:" + key + " ts:" + tsCliente +
                        ". Meu ts é " + dado.timestamp + ", portanto devolvendo WAIT_FOR_RESPONSE.");
                synchronized (getsPendentes) {
                    getsPendentes.computeIfAbsent(key, k -> new ArrayList<>()).add(
                        new Mensagem(msg.getTipo(), key, null, tsCliente, ipCliente, portaCliente)
                    );
                }
                Mensagem resposta = new Mensagem("WAIT_FOR_RESPONSE", key, null, dado.timestamp, null, 0);
                out.println(gson.toJson(resposta));
            }
        }

        /*
         * SEÇÃO 5d: REPLICATION — replica valor recebido do líder (TCP)
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
         * (GET assíncrono) — envia GET_OK para clientes pendentes assim que possível (5f)
         */
        private void notificarPendentes(String key, String value, long timestamp) {
            List<Mensagem> pendentes;
            synchronized (getsPendentes) {
                pendentes = getsPendentes.remove(key);
            }

            if (pendentes != null) {
                for (Mensagem pendente : pendentes) {
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
