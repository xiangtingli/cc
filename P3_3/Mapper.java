import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Mapper
{
	public static void main (String arg[])
	{
		try
		{
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			String input;
			while((input = br.readLine()) != null)
			{
				String[] strs = input.split(",");
				if(strs.length == 3)
				{
					System.out.println(strs[0] + '\t' + strs[1] + '\t' + strs[2]);
				}
			}
			br.close();
		}
		catch(IOException io)
		{
			io.printStackTrace();
		}
	}
}

