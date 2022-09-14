public class Partition implements Comparable<Partition> {

    private int id;
    private long lag;
    private double arrivalRate;
    private double previousArrivalRate;
    //TODO externlize windown length and add wondows for
    //TODO rate of arrival rate d/dt(arrival rate)
    // TODO and window for the lag rate d/dt (lag)


    public Partition(int id, long lag, double arrivalRate) {
        this.id = id;
        this.lag = lag;
        this.arrivalRate = arrivalRate;

    }





    public int getId() {
        return id;
    }
    public void setId(int id) {
        this.id = id;
    }
    public long getLag() {
        return lag;
    }

    public void setLag(long lag, boolean updateWindow) {
        this.lag = lag;
        if(!updateWindow) return;


        //rateForLagWindow[0] = (double) (lagWindow[0] - lagWindow[1]) / Controller.doublesleep;
    }

    public double getArrivalRate() {
        return arrivalRate;
    }

    public void setArrivalRate(double arrivalRate, boolean updateWindow) {
        this.arrivalRate = arrivalRate;


    }




    @Override
    public String toString() {
        return "Partition{" +
                "id= " + id +
                ", lag= " + lag +
                ", arrivalRate= " + String.format("%.2f", arrivalRate) +
                "}\n";
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Partition partition = (Partition) o;

        if (id != partition.id) return false;
        if (lag != partition.lag) return false;
        return Double.compare(partition.arrivalRate, arrivalRate) == 0;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = id;
        result = 31 * result + (int) (lag ^ (lag >>> 32));
        temp = Double.doubleToLongBits(arrivalRate);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }




    @Override
    public int compareTo(Partition o) {
        return Double.compare(arrivalRate, o.arrivalRate);
    }

    //TODO add corresponding windows for  lag rate (d/dt lag(t)), and a function to return its average etc...
    //TODO add corresponding window for rate of arrival rate, and a function to return the average rate of arrival rate
    //TODO customize and externalize parameters such as window size and wherever applicable
}
