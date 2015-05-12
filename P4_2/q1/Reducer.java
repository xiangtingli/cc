import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Reducer
{
	public static void main (String arg[])
	{
		try
		{
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			String input;
			while((input = br.readLine()) != null)
			{	
				String[] ss = input.split("\t");
				if(ss.length != 2)
				{
					continue;
				}
				System.out.println(ss[1] + "\t" + ss[0]);
			}
			br.close();
		}
		catch(IOException io)
		{
			io.printStackTrace();
		}
	}
}
