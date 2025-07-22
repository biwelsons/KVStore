public class Mensagem {
    private String tipo;

    private String key;
    private String value;

    private long timestamp;

    private String ipCliente;
    private int portaCliente;

    public Mensagem() {}

    public Mensagem(String tipo, String key, String value, long timestamp, String ipCliente, int portaCliente) {
        this.tipo = tipo;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.ipCliente = ipCliente;
        this.portaCliente = portaCliente;
    }

    public String getTipo() {
        return tipo;
    }

    public void setTipo(String tipo) {
        this.tipo = tipo;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getIpCliente() {
        return ipCliente;
    }

    public void setIpCliente(String ipCliente) {
        this.ipCliente = ipCliente;
    }

    public int getPortaCliente() {
        return portaCliente;
    }

    public void setPortaCliente(int portaCliente) {
        this.portaCliente = portaCliente;
    }
}
