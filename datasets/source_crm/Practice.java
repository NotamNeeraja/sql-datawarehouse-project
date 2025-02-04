class Practice{
  
 public static void main(String[] args){
   Object o=Class.forName(args[0]).newInstance();
   System.out.println(o);
  }
}