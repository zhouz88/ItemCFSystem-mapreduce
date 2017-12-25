

public class Driver {
    public static void main(String[] args) throws Exception{
        UserMovies usermovies = new UserMovies();
        BuildMatrix buildmatrix = new BuildMatrix();
        NormalizeMatrix nomalizematrix = new NormalizeMatrix();
        MatrixMultipicaliton matrixmultipicaliton = new MatrixMultipicaliton();
        Sum sum = new Sum();

        String input = args[0];
        String output1 = args[1];
        String output2 = args[2];
        String output3 = args[3];
        String output4 = args[4];
        String output5 = args[5];

        String[] args1 = {input, output1};
        String[] args2 = {output1, output2};
        String[] args3 = {output2, output3};
        String[] args4 = {output3, input,  output4};
        String[] args5 = {output4, output5};

//        usermovies.main(args1);
//        buildmatrix.main(args2);
//        nomalizematrix.main(args3);
        matrixmultipicaliton.main(args4);
        sum.main(args5);
    }
}
