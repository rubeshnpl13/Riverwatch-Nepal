import {
  Component,
  type ReactNode,
} from "react";


interface DashboardErrorBoundaryProps {
  children: ReactNode;
}


interface DashboardErrorBoundaryState {
  hasError: boolean;
}


export class DashboardErrorBoundary
  extends Component<
    DashboardErrorBoundaryProps,
    DashboardErrorBoundaryState
  > {

  state:
    DashboardErrorBoundaryState = {
      hasError: false,
    };


  static getDerivedStateFromError():
    DashboardErrorBoundaryState {
    return {
      hasError: true,
    };
  }


  render() {
    if (
      this.state.hasError
    ) {
      return (
        <main className="fatal-error-shell">
          <section
            className="fatal-error-card"
            role="alert"
          >
            <p className="section-label">
              RiverWatch Nepal
            </p>

            <h1>
              Dashboard unavailable
            </h1>

            <p>
              An unexpected interface
              error prevented the
              dashboard from rendering.
            </p>

            <button
              type="button"
              onClick={
                () => {
                  window.location
                    .reload();
                }
              }
            >
              Reload dashboard
            </button>
          </section>
        </main>
      );
    }

    return this.props.children;
  }
}