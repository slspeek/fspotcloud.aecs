package aecs;

import aecs.servlet.AECSServlet;

import com.google.inject.servlet.ServletModule;

public class AECSModule extends ServletModule {
	
	@Override
	protected void configureServlets() {
		serve("/_aecs/*").with(AECSServlet.class);
		serve("/_aecs").with(AECSServlet.class);
	}

}
