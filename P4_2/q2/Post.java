import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Post
{
	public static void main (String arg[])
	{
		try
		{
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			String input;
			while((input = br.readLine()) != null)
			{
				int pos = input.lastIndexOf(',');
				if(pos < 0)
				{
					continue;
				}
				String title = input.substring(1, pos);
				String rank = input.substring(pos + 1, input.length() - 1);
				System.out.println(title + "\t" + rank);
			}
			br.close();
		}
		catch(IOException io)
		{
			io.printStackTrace();
		}
	}
}	
