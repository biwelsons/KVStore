import com.google.gson.Gson;
import java.io.*;
import java.net.*;
import java.util.*;

/*
 * Projeto SD: ClienteX — Key-Value Store Distribuído
 * Funcionalidades implementadas conforme SEÇÃO 4 do enunciado.
 */
public class Cliente {
    private static final Scanner scanner = new Scanner(System.in);
    private static final Gson gson = new Gson();
    private static final Random random = new Random();

    // Lista de servidores conhecidos pelo cliente (capturada via INIT, item 4a)
    private static List<InetSocketAddress> servidores = new ArrayList<>();

    // Mapa de timestamps por chave para garantir consistência (item 4c)
    private static Map<String, Long> timestamps = new HashMap<>();

    // Porta do cliente para escutar respostas assíncronas (item 4c)
    private static int portaCliente = 0;

    public static void main(String[] args) throws UnsupportedEncodingException {
        while (true) {
            // Garante UTF-8 no terminal (opcional)
            System.setOut(new PrintStream(System.out, true, "UTF-8"));

            // Menu interativo obrigatório — 4a (apenas INIT, PUT, GET)
            System.out.println("\nEscolha uma opcao:");
            System.out.println("1 - INIT (inicializar cliente)");
            System.out.println("2 - PUT (inserir key-value)");
            System.out.println("3 - GET (consultar key)");
            System.out.print("Opcao: ");
            String opcao = scanner.nextLine().trim();

            switch (opcao) {
                case "1":
                    initCliente(); // SEÇÃO 4a: Inicialização
                    break;
                case "2":
                    put(); // SEÇÃO 4b: PUT
                    break;
                case "3":
                    get(); // SEÇÃO 4c: GET
                    break;
                default:
                    System.out.println("Opcao invalida.");
            }
        }
    }

    /*
     * SEÇÃO 4a: Inicialização do cliente
     * - O cliente captura do teclado os IPs e portas dos três servidores
     * - Não sabe quem é o líder!
     */
    private static void initCliente() {
        servidores.clear();
        for (int i = 1; i <= 3; i++) {
            System.out.print("Digite o IP do servidor " + i + ": ");
            String ip = scanner.nextLine().trim();
            System.out.print("Digite a porta do servidor " + i + ": ");
            int porta = Integer.parseInt(scanner.nextLine().trim());
            servidores.add(new InetSocketAddress(ip, porta));
        }
        // Captura porta para respostas assíncronas (GET com WAIT_FOR_RESPONSE)
        System.out.print("Digite a porta para receber respostas assincronas (ex: 20000): ");
        portaCliente = Integer.parseInt(scanner.nextLine().trim());
        iniciarServidorDeRetorno(portaCliente);

        System.out.println("Cliente inicializado com " + servidores.size() + " servidores.");
    }

    /*
     * SEÇÃO 4b: PUT
     * - Captura key/value do teclado, escolhe servidor aleatório
     * - Envia PUT (TCP) para qualquer servidor
     * - Espera mensagem PUT_OK com timestamp (4b, 5c)
     */
    private static void put() {
        if (servidores.isEmpty()) {
            System.out.println("Erro: Cliente nao inicializado (use opcao INIT primeiro).");
            return;
        }
        System.out.print("Digite a chave (key): ");
        String key = scanner.nextLine().trim();
        System.out.print("Digite o valor (value): ");
        String value = scanner.nextLine().trim();

        // Escolhe servidor aleatório (não sabe quem é o líder)
        InetSocketAddress servidor = escolherServidorAleatorio();
        Mensagem msg = new Mensagem("PUT", key, value, 0, null, 0);

        // Envia via TCP (item 4b, Observações)
        Mensagem resposta = enviarMensagem(servidor, msg);
        if (resposta != null && "PUT_OK".equals(resposta.getTipo())) {
            // Atualiza timestamp local
            timestamps.put(key, resposta.getTimestamp());
            // Print exato do enunciado!
            System.out.println("PUT_OK key: " + key +
                    " value " + value +
                    " timestamp " + resposta.getTimestamp() +
                    " realizada no servidor " + servidor.getAddress().getHostAddress() + ":" + servidor.getPort());
        } else {
            System.out.println("Falha no PUT.");
        }
    }

    /*
     * SEÇÃO 4c: GET
     * - Captura key do teclado
     * - Envia GET (TCP) para servidor aleatório
     * - Envia key, timestamp local e info do cliente (4c)
     * - Se não tiver key atualizada, cliente recebe WAIT_FOR_RESPONSE e recebe value depois, assíncrono
     */
    private static void get() {
        if (servidores.isEmpty()) {
            System.out.println("Erro: Cliente nao inicializado (use opcao INIT primeiro).");
            return;
        }
        System.out.print("Digite a chave (key) a ser buscada: ");
        String key = scanner.nextLine().trim();
        // Timestamp local (não vem do teclado!)
        long tsCliente = timestamps.getOrDefault(key, 0L);

        InetSocketAddress servidor = escolherServidorAleatorio();

        try {
            String ipLocal = InetAddress.getLocalHost().getHostAddress();
            Mensagem msg = new Mensagem("GET", key, null, tsCliente, ipLocal, portaCliente);
            Mensagem resposta = enviarMensagem(servidor, msg);

            if (resposta != null) {
                if ("WAIT_FOR_RESPONSE".equals(resposta.getTipo())) {
                    // Print exato do enunciado!
                    System.out.println("GET key: " + key + " WAIT_FOR_RESPONSE do servidor " +
                            servidor.getAddress().getHostAddress() + ":" + servidor.getPort());
                } else if ("GET_OK".equals(resposta.getTipo())) {
                    // Print exato do enunciado!
                    System.out.println("GET key: " + key +
                            " value: " + resposta.getValue() +
                            " obtido do servidor " + servidor.getAddress().getHostAddress() + ":" + servidor.getPort() +
                            ", meu timestamp " + tsCliente + " e do servidor " + resposta.getTimestamp());
                    timestamps.put(key, resposta.getTimestamp());
                } else {
                    System.out.println("Resposta inesperada: " + resposta.getTipo());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
     * Escolhe servidor aleatório para qualquer operação (4b, 4c)
     */
    private static InetSocketAddress escolherServidorAleatorio() {
        return servidores.get(random.nextInt(servidores.size()));
    }

    /*
     * Envia mensagem via TCP, recebe resposta — obrigatoriedade de TCP (Observações 4)
     */
    private static Mensagem enviarMensagem(InetSocketAddress servidor, Mensagem mensagem) {
        try (Socket socket = new Socket(servidor.getAddress(), servidor.getPort());
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            String json = gson.toJson(mensagem);
            out.println(json);

            String respostaJson = in.readLine();
            return gson.fromJson(respostaJson, Mensagem.class);

        } catch (IOException e) {
            System.out.println("Erro ao se conectar com o servidor " + servidor + ": " + e.getMessage());
            return null;
        }
    }

    /*
     * Listener assíncrono para respostas GET_OK futuras (quando recebeu WAIT_FOR_RESPONSE)
     * (4c, segunda parte)
     */
    private static void iniciarServidorDeRetorno(int porta) {
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(porta)) {
                System.out.println("Servidor de retorno escutando na porta " + porta + "...");
                while (true) {
                    Socket socket = serverSocket.accept();
                    new Thread(() -> {
                        try (
                            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
                        ) {
                            String respostaJson = in.readLine();
                            Mensagem resposta = gson.fromJson(respostaJson, Mensagem.class);

                            // Print exato do enunciado para resposta assíncrona
                            System.out.println("GET key: " + resposta.getKey() +
                                    " value: " + resposta.getValue() +
                                    " obtido do servidor [assincrono], meu timestamp " +
                                    timestamps.getOrDefault(resposta.getKey(), 0L) +
                                    " e do servidor " + resposta.getTimestamp());

                            timestamps.put(resposta.getKey(), resposta.getTimestamp());
                        } catch (IOException e) {
                            System.out.println("Erro ao receber resposta assincrona: " + e.getMessage());
                        }
                    }).start();
                }
            } catch (IOException e) {
                System.out.println("Erro no servidor de retorno: " + e.getMessage());
            }
        }).start();
    }
}
